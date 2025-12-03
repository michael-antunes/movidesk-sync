#!/usr/bin/env python

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Set

import psycopg2
from psycopg2.extras import Json
import requests

LOG_NAME = "abertos_index"
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


def ensure_open_table(conn) -> None:
    sql = """
    CREATE SCHEMA IF NOT EXISTS visualizacao_atual;

    CREATE TABLE IF NOT EXISTS visualizacao_atual.tickets_abertos (
        ticket_id       BIGINT PRIMARY KEY,
        raw             JSONB,
        updated_at      TIMESTAMPTZ NOT NULL,
        raw_last_update TIMESTAMPTZ
    );

    ALTER TABLE visualizacao_atual.tickets_abertos
        ALTER COLUMN raw DROP NOT NULL;

    ALTER TABLE visualizacao_atual.tickets_abertos
        ADD COLUMN IF NOT EXISTS raw_last_update TIMESTAMPTZ;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_existing_open_ids(conn) -> Set[int]:
    sql = "SELECT ticket_id FROM visualizacao_atual.tickets_abertos;"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {int(r[0]) for r in rows}


def fetch_merged_ticket_ids(conn) -> Set[int]:
    sql = "SELECT ticket_id FROM visualizacao_resolvidos.tickets_mesclados;"
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return {int(r[0]) for r in rows}
    except Exception as exc:
        logger.warning(
            "Não foi possível ler visualizacao_resolvidos.tickets_mesclados; "
            "seguindo sem exclusão de mesclados nesta execução: %s",
            exc,
        )
        conn.rollback()
        return set()


def delete_closed_from_open(conn, still_open_ids: Set[int]) -> int:
    existing_ids = fetch_existing_open_ids(conn)
    to_delete = existing_ids - still_open_ids
    if not to_delete:
        logger.info("Nenhum ticket a remover de tickets_abertos.")
        return 0

    logger.info("Removendo %s tickets que não estão mais abertos.", len(to_delete))
    sql = """
        DELETE FROM visualizacao_atual.tickets_abertos
        WHERE ticket_id = ANY(%s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(to_delete),))
    conn.commit()
    return len(to_delete)


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

    def list_open_tickets_page(self, skip: int, top: int) -> List[Dict[str, Any]]:
        full_select = "id,lastUpdate,baseStatus"

        base_status_filter = (
            "(baseStatus ne 'Resolved' and "
            "baseStatus ne 'Closed' and "
            "baseStatus ne 'Canceled')"
        )

        params = {
            "$filter": base_status_filter,
            "$orderby": "id",
            "$top": str(top),
            "$skip": str(skip),
            "$select": full_select,
            "includeDeletedItems": "true",
        }

        logger.info(
            "Listando tickets abertos em /tickets com skip=%s, top=%s",
            skip,
            top,
        )
        data = self._request("/tickets", params)

        if not isinstance(data, list):
            return []

        result: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict) and item.get("id"):
                result.append(item)
        return result


def upsert_open_index(conn, ticket_id: int, last_update: str) -> None:
    sql = """
        INSERT INTO visualizacao_atual.tickets_abertos (
            ticket_id,
            updated_at
        )
        VALUES (%s, %s)
        ON CONFLICT (ticket_id) DO UPDATE
        SET updated_at = EXCLUDED.updated_at;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_id, last_update))


def sync_open_index(conn, client: MovideskClient, page_size: int) -> Tuple[int, int, int]:
    ok = 0
    fail = 0
    skip = 0
    still_open_ids: Set[int] = set()

    merged_ids = fetch_merged_ticket_ids(conn)
    if merged_ids:
        logger.info(
            "Serão ignorados %s tickets presentes em visualizacao_resolvidos.tickets_mesclados.",
            len(merged_ids),
        )

    while True:
        tickets = client.list_open_tickets_page(skip, page_size)
        if not tickets:
            break

        try:
            for idx, ticket in enumerate(tickets, start=1):
                try:
                    ticket_id = int(ticket["id"])
                    last_update = ticket.get("lastUpdate")
                    if not last_update:
                        logger.warning(
                            "Ticket ID=%s sem lastUpdate na página skip=%s posição=%s: ignorando.",
                            ticket_id,
                            skip,
                            idx,
                        )
                        fail += 1
                        continue
                except Exception:
                    logger.warning(
                        "Ticket inválido na página skip=%s posição=%s: ignorando.",
                        skip,
                        idx,
                    )
                    fail += 1
                    continue

                if ticket_id in merged_ids:
                    logger.info(
                        "Ticket %s está em tickets_mesclados; ignorando em tickets_abertos.",
                        ticket_id,
                    )
                    continue

                upsert_open_index(conn, ticket_id, last_update)
                still_open_ids.add(ticket_id)
                ok += 1

            conn.commit()
        except Exception as exc:
            logger.exception(
                "Erro ao gravar página de tickets abertos (skip=%s) no Neon: %s",
                skip,
                exc,
            )
            conn.rollback()
            fail += len(tickets)

        skip += len(tickets)
        if len(tickets) < page_size:
            break

    removed = delete_closed_from_open(conn, still_open_ids)
    return ok, fail, removed


def main(argv: Optional[List[str]] = None) -> None:
    try:
        page_size = int(get_env("DETAIL_OPEN_PAGE_SIZE", "100"))
    except ValueError:
        page_size = 100

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sync de índice de tickets abertos (page_size=%s).",
        page_size,
    )

    conn = get_db_connection()
    ensure_open_table(conn)

    client = MovideskClient(token=token)

    ok, fail, removed = sync_open_index(conn, client, page_size)

    logger.info(
        "Sync índice concluído. Sucesso=%s, Falhas=%s, Removidos(fecharam/mesclaram)=%s.",
        ok,
        fail,
        removed,
    )

    conn.close()


if __name__ == "__main__":
    main()
