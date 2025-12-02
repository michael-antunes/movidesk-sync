#!/usr/bin/env python

import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json
import requests

LOG_NAME = "abertos_detail"
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
    CREATE SCHEMA IF NOT EXISTS visualizacao_atual;

    CREATE TABLE IF NOT EXISTS visualizacao_atual.tickets_abertos (
        ticket_id   BIGINT PRIMARY KEY,
        raw         JSONB NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def truncate_open_table(conn) -> None:
    sql = "TRUNCATE TABLE visualizacao_atual.tickets_abertos;"
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

    def list_open_tickets_page(self, skip: int, top: int) -> List[Dict[str, Any]]:
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


def upsert_ticket_detail_json(conn, ticket_id: int, ticket_json: Dict[str, Any]) -> None:
    sql = """
        INSERT INTO visualizacao_atual.tickets_abertos (
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


def sync_open_tickets(conn, client: MovideskClient, page_size: int) -> Tuple[int, int]:
    truncate_open_table(conn)

    ok = 0
    fail = 0
    skip = 0

    while True:
        tickets = client.list_open_tickets_page(skip, page_size)
        if not tickets:
            break

        for idx, ticket in enumerate(tickets, start=1):
            try:
                ticket_id = int(ticket["id"])
            except Exception:
                logger.warning(
                    "Ticket sem id numérico na página skip=%s posição=%s: ignorando.",
                    skip,
                    idx,
                )
                fail += 1
                continue

            logger.info(
                "Processando ticket aberto (ID=%s) na página skip=%s",
                ticket_id,
                skip,
            )

            try:
                upsert_ticket_detail_json(conn, ticket_id, ticket)
                conn.commit()
                ok += 1
            except Exception as exc:
                logger.exception(
                    "Erro ao gravar ticket aberto ticket_id=%s no Neon: %s",
                    ticket_id,
                    exc,
                )
                conn.rollback()
                fail += 1

        skip += len(tickets)
        if len(tickets) < page_size:
            break

    return ok, fail


def main(argv: Optional[List[str]] = None) -> None:
    try:
        page_size = int(get_env("DETAIL_OPEN_PAGE_SIZE", "100"))
    except ValueError:
        page_size = 100

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sincronização de tickets abertos (page_size=%s).",
        page_size,
    )

    conn = get_db_connection()
    ensure_detail_table_exists(conn)

    client = MovideskClient(token=token)

    ok, fail = sync_open_tickets(conn, client, page_size)

    logger.info(
        "Processamento concluído. Sucesso=%s, Falhas=%s.",
        ok,
        fail,
    )

    conn.close()


if __name__ == "__main__":
    main()
