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

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
THROTTLE = float(os.getenv("THROTTLE", "0.4"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("sync_resolved_detail")

# ---------------------------------------------------------------------------
# Conexão com o banco
# ---------------------------------------------------------------------------


def get_db_connection():
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não configurado")

    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = False

    # Ajuste aqui o schema caso o nome seja diferente
    with conn.cursor() as cur:
        try:
            cur.execute(
                "SET search_path TO tickets_movidesk, visualizacao_resolvidos, public"
            )
        except Exception as e:
            logger.warning("Não foi possível definir search_path customizado: %s", e)

    return conn


# ---------------------------------------------------------------------------
# Fila de missing (audit_recent_missing)
# ---------------------------------------------------------------------------


def fetch_missing_ticket_ids(cur, limit: int) -> List[int]:
    """
    Busca na audit_recent_missing os tickets marcados como faltando
    para a tabela 'tickets_resolvidos', pegando sempre o run mais recente
    de cada ticket.
    """
    cur.execute(
        """
        SELECT ticket_id
        FROM (
            SELECT
                m.ticket_id,
                r.run_at,
                ROW_NUMBER() OVER (
                    PARTITION BY m.ticket_id
                    ORDER BY r.run_at DESC
                ) AS rn
            FROM audit_recent_missing m
            JOIN audit_recent_run r
              ON r.id = m.run_id
            WHERE m.table_name = 'tickets_resolvidos'
        ) t
        WHERE rn = 1
        ORDER BY run_at DESC, ticket_id DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# API Movidesk
# ---------------------------------------------------------------------------

session = requests.Session()


def fetch_ticket_from_api(ticket_id: int) -> Optional[Dict[str, Any]]:
    """
    Busca um ticket específico na API do Movidesk.

    Usando o endpoint /tickets/{id}?token=...
    (igual à URL que você acessa no navegador).
    """
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    url = f"{API_BASE}/tickets/{ticket_id}"
    params = {
        "token": API_TOKEN,
        "$expand": (
            "owner,organization,clients,"
            "customFieldValues,customFieldValues($expand=items)"
        ),
    }

    try:
        resp = session.get(url, params=params, timeout=30)
    except Exception as exc:
        logger.error("Erro de conexão ao buscar ticket %s: %s", ticket_id, exc)
        return None

    if resp.status_code == 404:
        logger.warning("ticket %s não encontrado na API (404).", ticket_id)
        return None

    if not resp.ok:
        logger.error(
            "ticket %s: erro HTTP %s da API: %s",
            ticket_id,
            resp.status_code,
            resp.text[:300],
        )
        return None

    try:
        data = resp.json()
    except Exception as exc:
        logger.error("ticket %s: erro ao parsear JSON: %s", ticket_id, exc)
        return None

    if not isinstance(data, dict):
        logger.error("ticket %s: JSON inesperado: %r", ticket_id, data)
        return None

    return data


def parse_datetime(value: Optional[str]) -> Optional[str]:
    """
    Deixa o Postgres converter a string ISO.
    """
    if not value:
        return None
    return value


def extract_resolved_closed_dates(ticket: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Lê os campos resolvedIn e closedIn do JSON do Movidesk
    e devolve como last_resolved_at / last_closed_at.
    """
    resolved_in = ticket.get("resolvedIn")
    closed_in = ticket.get("closedIn")

    last_resolved_at = parse_datetime(resolved_in)
    last_closed_at = parse_datetime(closed_in)

    return {
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
    }


# ---------------------------------------------------------------------------
# Persistência em visualizacao_resolvidos.tickets_resolvidos
# ---------------------------------------------------------------------------


def update_dates_in_db(cur, rows: List[Dict[str, Any]]) -> int:
    """
    Atualiza last_resolved_at e last_closed_at em visualizacao_resolvidos.tickets_resolvidos.

    rows: lista de dicts com:
      - ticket_id
      - last_resolved_at
      - last_closed_at
    """
    if not rows:
        return 0

    params = [
        (row["last_resolved_at"], row["last_closed_at"], row["ticket_id"])
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

    return cur.rowcount


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    logger.info("Iniciando sync_resolved_detail (detail)...")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            ticket_ids = fetch_missing_ticket_ids(cur, BATCH_SIZE)

            if not ticket_ids:
                logger.info(
                    "Nenhum ticket em audit_recent_missing para 'tickets_resolvidos'."
                )
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
                time.sleep(THROTTLE)

                if ticket is None:
                    # não conseguimos dados; mantemos esse ticket na fila de missing
                    continue

                dates = extract_resolved_closed_dates(ticket)

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
                logger.info(
                    "Nenhuma data de resolução/fechamento para atualizar em tickets_resolvidos."
                )

            conn.commit()

    except Exception as exc:
        logger.critical("Erro ao executar sync_resolved_detail: %s", exc, exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
