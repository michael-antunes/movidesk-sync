import os
import time
import logging
from typing import List, Optional, Dict, Any

import requests
import psycopg2
import psycopg2.extras


API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

# Quantidade de tickets por batch a partir da fila de missing
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
# Intervalo entre chamadas à API (segundos) pra não estourar limite
THROTTLE = float(os.getenv("THROTTLE", "0.4"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("sync_resolved_detail")


def get_db_connection():
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não configurado")
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = False
    return conn


def fetch_missing_ticket_ids(cur, limit: int) -> List[int]:
    """
    Busca na audit_recent_missing os tickets marcados como faltando
    para a tabela 'tickets_resolvidos', ordenando pelos runs mais recentes.
    """
    cur.execute(
        """
        SELECT DISTINCT m.ticket_id
        FROM audit_recent_missing m
        JOIN audit_recent_run r
          ON r.id = m.run_id
        WHERE m.table_name = 'tickets_resolvidos'
        ORDER BY r.run_at DESC, m.ticket_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return [row[0] for row in rows]


session = requests.Session()


def fetch_ticket_from_api(ticket_id: int) -> Optional[Dict[str, Any]]:
    """
    Busca um ticket específico na API do Movidesk.

    IMPORTANTE: endpoint correto é /tickets com o parâmetro ?id=
    Ex: /public/v1/tickets?token=...&id=295148
    """
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        # Garantir que owner, organização e custom fields venham completos
        "$expand": "owner,organization,clients,customFieldValues,customFieldValues($expand=items)",
    }

    url = f"{API_BASE}/tickets"

    try:
        resp = session.get(url, params=params, timeout=30)
    except Exception as exc:
        logger.error("Erro de conexão ao buscar ticket %s: %s", ticket_id, exc)
        return None

    if resp.status_code == 404:
        # Ticket não encontrado na API, mas existe nas ações / DB
        logger.warning("ticket %s não encontrado na API (404).", ticket_id)
        return None

    if not resp.ok:
        logger.error(
            "Erro HTTP ao buscar ticket %s: status=%s body=%s",
            ticket_id,
            resp.status_code,
            resp.text[:500],
        )
        return None

    try:
        data = resp.json()
    except Exception as exc:
        logger.error("Erro ao parsear JSON do ticket %s: %s", ticket_id, exc)
        return None

    # A API pode devolver um único objeto ou uma lista com 1 item
    if isinstance(data, list):
        if not data:
            logger.warning("ticket %s: resposta vazia da API.", ticket_id)
            return None
        return data[0]

    if not isinstance(data, dict):
        logger.error("ticket %s: JSON inesperado: %r", ticket_id, data)
        return None

    return data


def parse_datetime(value: Optional[str]) -> Optional[str]:
    """
    Mantém simples: se vier string ISO, o Postgres converte.
    Se vier vazio ou None, retorna None.
    """
    if not value:
        return None
    return value


def extract_resolved_closed_dates(ticket: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Lê os campos resolvedIn e closedIn da API do Movidesk
    e converte para last_resolved_at e last_closed_at.
    """
    resolved_in = ticket.get("resolvedIn")
    closed_in = ticket.get("closedIn")

    last_resolved_at = parse_datetime(resolved_in)
    last_closed_at = parse_datetime(closed_in)

    return {
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
    }


def update_dates_in_db(cur, rows: List[Dict[str, Any]]) -> int:
    """
    Atualiza last_resolved_at e last_closed_at em visualizacao_resolvidos.tickets_resolvidos
    para os tickets informados.

    rows: lista de dicts com keys: ticket_id, last_resolved_at, last_closed_at
    """
    if not rows:
        return 0

    params = [
        (
            row["last_resolved_at"],
            row["last_closed_at"],
            row["ticket_id"],
        )
        for row in rows
    ]

    cur.executemany(
        """
        UPDATE visualizacao_resolvidos.tickets_resolvidos
           SET last_resolved_at = COALESCE(%s, last_resolved_at),
               last_closed_at  = COALESCE(%s, last_closed_at)
         WHERE ticket_id = %s
        """,
        params,
    )

    # rowcount aqui é o total de linhas afetadas por todos os updates
    return cur.rowcount


def main():
    logger.info("Iniciando sync_resolved_detail (detail)...")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            ticket_ids = fetch_missing_ticket_ids(cur, BATCH_SIZE)

            if not ticket_ids:
                logger.info("Nenhum ticket em audit_recent_missing para tickets_resolvidos.")
                conn.commit()
                return

            logger.info(
                "Reprocessando %d tickets da audit_recent_missing (mais novos primeiro): %s",
                len(ticket_ids),
                ticket_ids,
            )

            updates: List[Dict[str, Any]] = []

            for ticket_id in ticket_ids:
                ticket = fetch_ticket_from_api(ticket_id)
                # Respeitar limite de requisições
                time.sleep(THROTTLE)

                if ticket is None:
                    # Não conseguimos dados; deixamos nas filas
                    continue

                dates = extract_resolved_closed_dates(ticket)

                # Se a API não trouxe nenhuma das datas, não adianta atualizar
                if not dates["last_resolved_at"] and not dates["last_closed_at"]:
                    logger.warning(
                        "ticket %s: API não retornou resolvedIn/closedIn (status=%s).",
                        ticket_id,
                        ticket.get("status"),
                    )
                    continue

                updates.append(
                    {
                        "ticket_id": ticket_id,
                        "last_resolved_at": dates["last_resolved_at"],
                        "last_closed_at": dates["last_closed_at"],
                    }
                )

            if updates:
                linhas = update_dates_in_db(cur, updates)
                logger.info("UPSERT detail: %d linhas atualizadas.", linhas)
            else:
                logger.info("Nenhuma data para atualizar em tickets_resolvidos.")

            conn.commit()

            # IMPORTANTE:
            # As triggers em tickets_resolvidos (tr_clear_audit_on_ticket_upsert, audit_missing_trigger, etc.)
            # é que vão limpar os registros em audit_recent_missing e alimentar o audit_ticket_watch
            # quando ainda faltar alguma informação (ex.: CSAT vazio, cancelado sem data, etc.).

    except Exception as exc:
        logger.fatal("Erro ao executar sync_resolved_detail: %s", exc, exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
