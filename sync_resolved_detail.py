#!/usr/bin/env python
"""
Sincroniza detalhes de tickets resolvidos/fechados/cancelados.

Fluxo:

1. Lê da fila visualizacao_resolvidos.audit_recent_missing
   (apenas registros com table_name = 'tickets_resolvidos').

2. Para cada ticket_id:
   - Tenta buscar em /tickets?id={ticket_id}&includeDeletedItems=true
     (JSON completo do ticket).
   - Se algum dia /tickets não retornar (404/vazio), tenta /tickets/past
     com um filtro simples por id.

3. Salva o JSON completo em visualizacao_resolvidos.tickets_resolvidos_detail
   (colunas: ticket_id, raw, updated_at).

4. Em caso de sucesso, remove o ticket_id da audit_recent_missing.
   Em caso de erro, mantém na fila para tentar de novo no próximo ciclo.

Configuração por variáveis de ambiente:
- NEON_DSN         : string de conexão para o PostgreSQL (Neon)
- MOVIDESK_TOKEN   : token da API do Movidesk
- PAGES_UPSERT     : limite de tickets por execução (default = 7)
"""

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import Json
import requests

# ---------------------------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------------------------

LOG_NAME = "detail"
BASE_URL = "https://api.movidesk.com/public/v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(LOG_NAME)


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        logger.error("Variável de ambiente obrigatória não definida: %s", name)
        sys.exit(1)
    return value  # type: ignore[return-value]


def get_db_connection():
    dsn = get_env("NEON_DSN", required=True)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def ensure_detail_table_exists(conn) -> None:
    """
    Garante a existência da tabela que armazena o JSON completo dos tickets.

    Obs.: se você já tiver criado uma tabela diferente, pode adaptar este
    trecho (nome da tabela/colunas) conforme sua estrutura.
    """
    sql = """
    CREATE SCHEMA IF NOT EXISTS visualizacao_resolvidos;

    CREATE TABLE IF NOT EXISTS visualizacao_resolvidos.tickets_resolvidos_detail (
        ticket_id   BIGINT PRIMARY KEY,
        raw         JSONB NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


# ---------------------------------------------------------------------------
# Leitura da fila audit_recent_missing
# ---------------------------------------------------------------------------

def fetch_pending_from_audit(conn, limit: int) -> List[int]:
    """
    Busca ticket_ids pendentes na fila audit_recent_missing
    para a tabela 'tickets_resolvidos'.
    """
    sql = """
        SELECT arm.ticket_id
        FROM visualizacao_resolvidos.audit_recent_missing AS arm
        WHERE arm.table_name = 'tickets_resolvidos'
        ORDER BY arm.ticket_id
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return [int(r[0]) for r in rows]


def remove_from_audit(conn, ticket_id: int) -> None:
    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = 'tickets_resolvidos'
          AND ticket_id   = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_id,))


# ---------------------------------------------------------------------------
# Cliente Movidesk
# ---------------------------------------------------------------------------

class MovideskClient:
    def __init__(self, token: str, timeout: int = 30) -> None:
        self.token = token
        self.timeout = timeout

    def _request(self, path: str, params: Dict[str, Any]) -> Optional[Any]:
        params = dict(params)
        params["token"] = self.token
        url = BASE_URL + path

        resp = requests.get(url, params=params, timeout=self.timeout)
        logger.info("GET %s -> %s", resp.url, resp.status_code)

        # 404 = não encontrado; tratamos como None
        if resp.status_code == 404:
            logger.warning(
                "Endpoint %s retornou 404 (não encontrado).", path
            )
            return None

        # Outros erros HTTP
        if resp.status_code >= 400:
            body_short = resp.text.replace("\n", " ")[:500]
            logger.error(
                "Erro HTTP ao chamar %s (status=%s): %s",
                path,
                resp.status_code,
                body_short,
            )
            # Deixa o chamador decidir o que fazer
            return None

        if not resp.text.strip():
            logger.warning("Resposta vazia em %s", path)
            return None

        try:
            return resp.json()
        except Exception:
            logger.exception("Falha ao parsear JSON da resposta de %s", path)
            return None

    def get_ticket(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        """
        Tenta obter o ticket primeiro em /tickets?id=...
        Se algum dia não vier, tenta /tickets/past com filtro por id.
        Retorna o JSON do ticket (dict) ou None.
        """
        # 1) /tickets?id=...
        params = {
            "id": str(ticket_id),
            "includeDeletedItems": "true",
        }
        logger.info(
            "Tentando buscar ticket %s em /tickets (id=...)", ticket_id
        )
        data = self._request("/tickets", params)

        if isinstance(data, list):
            # Por garantia, se a API devolver lista, pega o primeiro
            data = data[0] if data else None

        if isinstance(data, dict) and data.get("id"):
            return data  # sucesso

        # 2) Fallback: /tickets/past com filtro por id
        logger.info(
            "Ticket %s não encontrado em /tickets; tentando /tickets/past",
            ticket_id,
        )

        # Primeiro, tentamos SEM $select (pra ver se a API aceita e devolve tudo)
        params_past = {
            "$filter": f"id eq {ticket_id}",
        }
        data_past = self._request("/tickets/past", params_past)

        if data_past is None:
            # Se der erro ou nada, tenta de novo com $select explícito
            full_select = (
                "id,protocol,type,subject,category,urgency,status,baseStatus,"
                "justification,origin,createdDate,isDeleted,originEmailAccount,"
                "owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
                "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,"
                "resolvedIn,closedIn,canceledIn,actionCount,lifeTimeWorkingTime,"
                "stoppedTime,stoppedTimeWorkingTime,resolvedInFirstCall,"
                "chatWidget,chatGroup,chatTalkTime,chatWaitingTime,sequence,"
                "slaAgreement,slaAgreementRule,slaSolutionTime,slaResponseTime,"
                "slaSolutionChangedByUser,slaSolutionChangedBy,slaSolutionDate,"
                "slaSolutionDateIsPaused,jiraIssueKey,redmineIssueId,"
                "movideskTicketNumber,linkedToIntegratedTicketNumber,"
                "reopenedIn,lastActionDate,lastUpdate,slaResponseDate,"
                "slaRealResponseDate,clients,actions,parentTickets,"
                "childrenTickets,ownerHistories,statusHistories,"
                "satisfactionSurveyResponses,customFieldValues,assets,"
                "webhookEvents"
            )
            logger.info(
                "Re-tentando /tickets/past para ticket %s com $select explícito",
                ticket_id,
            )
            params_past["$select"] = full_select
            data_past = self._request("/tickets/past", params_past)

        if isinstance(data_past, list):
            data_past = data_past[0] if data_past else None

        if isinstance(data_past, dict) and data_past.get("id"):
            return data_past

        # Não encontrou em nenhum endpoint
        return None


# ---------------------------------------------------------------------------
# Upsert de detalhes no Neon
# ---------------------------------------------------------------------------

def upsert_ticket_detail_json(conn, ticket_id: int, ticket_json: Dict[str, Any]) -> None:
    """
    Salva (ou atualiza) o JSON completo do ticket na tabela de detalhes.
    """
    sql = """
        INSERT INTO visualizacao_resolvidos.tickets_resolvidos_detail (
            ticket_id,
            raw,
            updated_at
        )
        VALUES (%s, %s, now())
        ON CONFLICT (ticket_id) DO UPDATE
        SET raw        = EXCLUDED.raw,
            updated_at = EXCLUDED.updated_at;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_id, Json(ticket_json)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    # Limite de tickets por execução
    try:
        limit = int(get_env("PAGES_UPSERT", "7"))
    except ValueError:
        limit = 7

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sincronização de detalhes de tickets (limite=%s).", limit
    )

    conn = get_db_connection()
    ensure_detail_table_exists(conn)

    pending_ids = fetch_pending_from_audit(conn, limit)

    if not pending_ids:
        logger.info("Nenhum ticket pendente na audit_recent_missing.")
        conn.close()
        return

    logger.info(
        "%s tickets pendentes para atualização de detalhes (limite=%s).",
        len(pending_ids),
        limit,
    )
    if pending_ids:
        sample = ", ".join(str(i) for i in pending_ids[:5])
        logger.info("Primeiros pendentes (até 5): %s", sample)

    client = MovideskClient(token=token)

    ok = 0
    fail = 0

    for idx, ticket_id in enumerate(pending_ids, start=1):
        logger.info(
            "Processando ticket %s/%s (ID=%s)",
            idx,
            len(pending_ids),
            ticket_id,
        )

        try:
            ticket = client.get_ticket(ticket_id)
        except Exception as exc:  # erro inesperado de rede etc.
            logger.exception(
                "Erro inesperado ao buscar ticket_id=%s: %s",
                ticket_id,
                exc,
            )
            conn.rollback()
            fail += 1
            continue

        if ticket is None:
            logger.warning(
                "Ticket %s não encontrado em nenhum endpoint. "
                "Mantendo na fila para nova tentativa.",
                ticket_id,
            )
            fail += 1
            # não remove da audit_recent_missing
            continue

        # Temos o JSON completo do ticket: salva e remove da fila
        try:
            upsert_ticket_detail_json(conn, ticket_id, ticket)
            remove_from_audit(conn, ticket_id)
            conn.commit()
            ok += 1
            logger.info(
                "Ticket %s processado com sucesso (JSON salvo e removido da fila).",
                ticket_id,
            )
        except Exception as exc:
            logger.exception(
                "Erro ao gravar detalhes do ticket_id=%s no Neon: %s",
                ticket_id,
                exc,
            )
            conn.rollback()
            fail += 1

    logger.info(
        "Processamento concluído. Sucesso=%s, Falhas=%s.",
        ok,
        fail,
    )

    conn.close()


if __name__ == "__main__":
    main()
