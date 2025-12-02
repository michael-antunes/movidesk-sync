#!/usr/bin/env python
"""
Sincroniza detalhes de tickets resolvidos/fechados/cancelados.

Fluxo:

1. Busca em /tickets até DETAIL_BULK_LIMIT tickets após o último ticket_id
   gravado em visualizacao_resolvidos.tickets_resolvidos_detail.

2. Em seguida lê até DETAIL_MISSING_LIMIT itens da fila
   visualizacao_resolvidos.audit_recent_missing (table_name = 'tickets_resolvidos').

3. A exclusão da fila audit_recent_missing é feita por TRIGGER no banco
   ao inserir/atualizar em tickets_resolvidos_detail.

Configuração por variáveis de ambiente:
- NEON_DSN            : string de conexão para o PostgreSQL (Neon)
- MOVIDESK_TOKEN      : token da API do Movidesk
- DETAIL_BULK_LIMIT   : quantidade de tickets novos por execução (default = 200)
- DETAIL_MISSING_LIMIT: quantidade de tickets da fila missing por execução (default = 10)
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json
import requests

LOG_NAME = "detail"
BASE_URL = "https://api.movidesk.com/public/v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(LOG_NAME)


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


def get_last_ticket_id(conn) -> int:
    sql = """
        SELECT COALESCE(MAX(ticket_id), 0)
        FROM visualizacao_resolvidos.tickets_resolvidos_detail;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def fetch_pending_from_audit(conn, limit: int) -> List[int]:
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

        if resp.status_code == 404:
            logger.warning("Endpoint %s retornou 404 (não encontrado).", path)
            return None

        if resp.status_code >= 400:
            body_short = resp.text.replace("\n", " ")[:500]
            logger.error(
                "Erro HTTP ao chamar %s (status=%s): %s",
                path,
                resp.status_code,
                body_short,
            )
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
        params = {
            "id": str(ticket_id),
            "includeDeletedItems": "true",
        }
        logger.info("Tentando buscar ticket %s em /tickets (id=...)", ticket_id)
        data = self._request("/tickets", params)

        if isinstance(data, list):
            data = data[0] if data else None

        if isinstance(data, dict) and data.get("id"):
            return data

        logger.info(
            "Ticket %s não encontrado em /tickets; tentando /tickets/past",
            ticket_id,
        )

        params_past = {
            "$filter": f"id eq {ticket_id}",
        }
        data_past = self._request("/tickets/past", params_past)

        if data_past is None:
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

        return None

    def list_tickets_after(self, last_id: int, limit: int) -> List[Dict[str, Any]]:
        params = {
            "$filter": f"id gt {last_id}",
            "$orderby": "id",
            "$top": str(limit),
            "includeDeletedItems": "true",
        }
        logger.info(
            "Listando até %s tickets em /tickets com id > %s",
            limit,
            last_id,
        )
        data = self._request("/tickets", params)

        if not isinstance(data, list):
            return []

        result: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict) and item.get("id"):
                result.append(item)
        return result


def upsert_ticket_detail_json(conn, ticket_id: int, ticket_json: Dict[str, Any]) -> None:
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


def sync_new_tickets(conn, client: MovideskClient, limit: int) -> Tuple[int, int]:
    last_id = get_last_ticket_id(conn)
    logger.info(
        "Último ticket_id em tickets_resolvidos_detail: %s",
        last_id,
    )

    tickets = client.list_tickets_after(last_id, limit)

    if not tickets:
        logger.info("Nenhum ticket novo encontrado após id=%s.", last_id)
        return 0, 0

    ok = 0
    fail = 0

    for idx, ticket in enumerate(tickets, start=1):
        try:
            ticket_id = int(ticket["id"])
        except Exception:
            logger.warning("Ticket sem id numérico na posição %s: ignorando.", idx)
            fail += 1
            continue

        logger.info(
            "Processando ticket novo %s/%s (ID=%s)",
            idx,
            len(tickets),
            ticket_id,
        )

        try:
            upsert_ticket_detail_json(conn, ticket_id, ticket)
            conn.commit()
            ok += 1
            logger.info(
                "Ticket novo %s gravado em tickets_resolvidos_detail.",
                ticket_id,
            )
        except Exception as exc:
            logger.exception(
                "Erro ao gravar ticket novo ticket_id=%s no Neon: %s",
                ticket_id,
                exc,
            )
            conn.rollback()
            fail += 1

    return ok, fail


def sync_missing_tickets(conn, client: MovideskClient, limit: int) -> Tuple[int, int]:
    pending_ids = fetch_pending_from_audit(conn, limit)

    if not pending_ids:
        logger.info("Nenhum ticket pendente na audit_recent_missing.")
        return 0, 0

    logger.info(
        "%s tickets pendentes na fila audit_recent_missing (limite=%s).",
        len(pending_ids),
        limit,
    )
    sample = ", ".join(str(i) for i in pending_ids[:5])
    logger.info("Primeiros pendentes (até 5): %s", sample)

    ok = 0
    fail = 0

    for idx, ticket_id in enumerate(pending_ids, start=1):
        logger.info(
            "Processando ticket pendente %s/%s (ID=%s)",
            idx,
            len(pending_ids),
            ticket_id,
        )

        try:
            ticket = client.get_ticket(ticket_id)
        except Exception as exc:
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
                "Ticket %s não encontrado em nenhum endpoint. Mantendo na fila.",
                ticket_id,
            )
            fail += 1
            continue

        try:
            upsert_ticket_detail_json(conn, ticket_id, ticket)
            conn.commit()
            ok += 1
            logger.info(
                "Ticket pendente %s gravado em tickets_resolvidos_detail.",
                ticket_id,
            )
        except Exception as exc:
            logger.exception(
                "Erro ao gravar ticket pendente ticket_id=%s no Neon: %s",
                ticket_id,
                exc,
            )
            conn.rollback()
            fail += 1

    return ok, fail


def main(argv: Optional[List[str]] = None) -> None:
    try:
        bulk_limit = int(get_env("DETAIL_BULK_LIMIT", "200"))
    except ValueError:
        bulk_limit = 200

    try:
        missing_limit = int(get_env("DETAIL_MISSING_LIMIT", "10"))
    except ValueError:
        missing_limit = 10

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sincronização de detalhes de tickets (bulk=%s, missing=%s).",
        bulk_limit,
        missing_limit,
    )

    conn = get_db_connection()
    ensure_detail_table_exists(conn)

    client = MovideskClient(token=token)

    ok_new, fail_new = sync_new_tickets(conn, client, bulk_limit)
    ok_missing, fail_missing = sync_missing_tickets(conn, client, missing_limit)

    ok_total = ok_new + ok_missing
    fail_total = fail_new + fail_missing

    logger.info(
        "Processamento concluído. Sucesso=%s (novos=%s, missing=%s), Falhas=%s (novos=%s, missing=%s).",
        ok_total,
        ok_new,
        ok_missing,
        fail_total,
        fail_new,
        fail_missing,
    )

    conn.close()


if __name__ == "__main__":
    main()
