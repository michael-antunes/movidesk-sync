#!/usr/bin/env python

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json
import requests

LOG_NAME = "abertos_raw"
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

        if isinstance(data_past, list):
            data_past = data_past[0] if data_past else None

        if isinstance(data_past, dict) and data_past.get("id"):
            return data_past

        return None


def fetch_pending_for_raw(conn, limit: int) -> List[Tuple[int, Any, Optional[Any]]]:
    sql = """
        SELECT ta.ticket_id,
               ta.updated_at,
               ta.raw_last_update
        FROM visualizacao_atual.tickets_abertos ta
        LEFT JOIN visualizacao_resolvidos.tickets_mesclados tm
               ON tm.ticket_id = ta.ticket_id
        WHERE tm.ticket_id IS NULL
          AND (
                ta.raw IS NULL
             OR ta.raw_last_update IS NULL
             OR ta.updated_at > ta.raw_last_update
          )
        ORDER BY ta.updated_at
        LIMIT %s;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning(
            "Falha ao consultar tickets_mesclados; usando fallback sem JOIN: %s",
            exc,
        )
        conn.rollback()
        sql_fallback = """
            SELECT ticket_id,
                   updated_at,
                   raw_last_update
            FROM visualizacao_atual.tickets_abertos
            WHERE raw IS NULL
               OR raw_last_update IS NULL
               OR updated_at > raw_last_update
            ORDER BY updated_at
            LIMIT %s;
        """
        with conn.cursor() as cur:
            cur.execute(sql_fallback, (limit,))
            rows = cur.fetchall()

    result: List[Tuple[int, Any, Optional[Any]]] = []
    for r in rows:
        ticket_id = int(r[0])
        updated_at = r[1]
        raw_last = r[2]
        result.append((ticket_id, updated_at, raw_last))
    return result


def update_raw(conn, ticket_id: int, ticket_json: Dict[str, Any], updated_at: Any) -> None:
    sql = """
        UPDATE visualizacao_atual.tickets_abertos
        SET raw             = %s,
            raw_last_update = %s
        WHERE ticket_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (Json(ticket_json), updated_at, ticket_id))


def sync_open_raw(conn, client: MovideskClient, batch_size: int) -> Tuple[int, int]:
    total_ok = 0
    total_fail = 0

    while True:
        pending = fetch_pending_for_raw(conn, batch_size)

        if not pending:
            logger.info("Nenhum ticket aberto pendente de atualização de raw.")
            break

        logger.info(
            "Encontrados %s tickets pendentes de raw nesta rodada.",
            len(pending),
        )

        for idx, (ticket_id, updated_at, raw_last) in enumerate(pending, start=1):
            logger.info(
                "Processando raw de ticket aberto %s/%s (ID=%s, updated_at=%s, raw_last_update=%s)",
                idx,
                len(pending),
                ticket_id,
                updated_at,
                raw_last,
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
                total_fail += 1
                continue

            if ticket is None:
                logger.warning(
                    "Ticket %s não encontrado em nenhum endpoint. Mantendo pendente.",
                    ticket_id,
                )
                total_fail += 1
                continue

            try:
                update_raw(conn, ticket_id, ticket, updated_at)
                conn.commit()
                total_ok += 1
                logger.info(
                    "Raw do ticket aberto %s atualizado com sucesso.",
                    ticket_id,
                )
            except Exception as exc:
                logger.exception(
                    "Erro ao gravar raw do ticket_id=%s no Neon: %s",
                    ticket_id,
                    exc,
                )
                conn.rollback()
                total_fail += 1

    return total_ok, total_fail


def main(argv: Optional[List[str]] = None) -> None:
    try:
        raw_limit = int(get_env("DETAIL_OPEN_RAW_LIMIT", "20"))
    except ValueError:
        raw_limit = 20

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sync de raw de tickets abertos (batch_size=%s).",
        raw_limit,
    )

    conn = get_db_connection()
    ensure_open_table(conn)

    client = MovideskClient(token=token)

    ok, fail = sync_open_raw(conn, client, raw_limit)

    logger.info(
        "Sync raw concluído. Sucesso=%s, Falhas=%s.",
        ok,
        fail,
    )

    conn.close()


if __name__ == "__main__":
    main()
