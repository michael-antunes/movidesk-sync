#!/usr/bin/env python
import logging
import os
import sys
import time
import random
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
        ORDER BY arm.ticket_id DESC
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def try_remove_from_audit(conn, ticket_id: int) -> None:
    """
    Opcional: remove da fila 'audit_recent_missing' quando conseguir gravar o detalhe.
    Se a tabela/colunas não existirem, ignora sem quebrar o job.
    """
    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = 'tickets_resolvidos' AND ticket_id = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ticket_id,))
    except Exception:
        conn.rollback()  # volta só esta tentativa, sem derrubar
        logger.debug("Não foi possível remover ticket %s da audit_recent_missing (ignorando).", ticket_id)


class MovideskClient:
    def __init__(
        self,
        token: str,
        connect_timeout: int = 10,
        read_timeout: int = 120,
        max_attempts: int = 6,
    ) -> None:
        self.token = token
        self.timeout = (connect_timeout, read_timeout)
        self.max_attempts = max_attempts

        self.session = requests.Session()
        retry = Retry(
            total=0,  # retry “real” fica no nosso loop, aqui deixamos 0 para não duplicar
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _request(self, path: str, params: Dict[str, Any]) -> Optional[Any]:
        params = dict(params)
        params["token"] = self.token
        url = BASE_URL + path

        # backoff base (com jitter)
        base_sleep = 1.5

        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                logger.info("GET %s -> %s (attempt %s/%s)", resp.url, resp.status_code, attempt, self.max_attempts)

                # 404: não existe
                if resp.status_code == 404:
                    logger.warning("Endpoint %s retornou 404.", path)
                    return None

                # 429: respeita retry-after quando vier
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    sleep_s = float(ra) if ra and ra.isdigit() else (base_sleep * (2 ** (attempt - 1)))
                    sleep_s += random.uniform(0, 0.8)
                    logger.warning("429 Rate limit. Dormindo %.1fs e tentando novamente...", sleep_s)
                    time.sleep(sleep_s)
                    continue

                # 5xx: retry com backoff
                if 500 <= resp.status_code <= 599:
                    if attempt == self.max_attempts:
                        body_short = (resp.text or "").replace("\n", " ")[:500]
                        logger.error("Erro HTTP %s em %s: %s", resp.status_code, path, body_short)
                        return None
                    sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                    logger.warning("HTTP %s. Dormindo %.1fs e tentando novamente...", resp.status_code, sleep_s)
                    time.sleep(sleep_s)
                    continue

                # 4xx: não adianta retry (exceto 429 que já tratamos)
                if resp.status_code >= 400:
                    body_short = (resp.text or "").replace("\n", " ")[:500]
                    logger.error("Erro HTTP %s ao chamar %s: %s", resp.status_code, path, body_short)
                    return None

                # resposta vazia
                if not (resp.text or "").strip():
                    if attempt == self.max_attempts:
                        logger.warning("Resposta vazia em %s após %s tentativas.", path, self.max_attempts)
                        return None
                    sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                    logger.warning("Resposta vazia. Dormindo %.1fs e tentando novamente...", sleep_s)
                    time.sleep(sleep_s)
                    continue

                try:
                    return resp.json()
                except Exception:
                    if attempt == self.max_attempts:
                        logger.exception("Falha ao parsear JSON de %s.", path)
                        return None
                    sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                    logger.warning("JSON inválido. Dormindo %.1fs e tentando novamente...", sleep_s)
                    time.sleep(sleep_s)
                    continue

            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as exc:
                if attempt == self.max_attempts:
                    logger.error("Timeout em %s (attempt %s/%s): %s", path, attempt, self.max_attempts, exc)
                    raise
                sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                logger.warning("Timeout em %s. Dormindo %.1fs e tentando novamente...", path, sleep_s)
                time.sleep(sleep_s)
                continue
            except requests.exceptions.RequestException as exc:
                # erro de rede/conexão genérico
                if attempt == self.max_attempts:
                    logger.error("Erro de rede em %s: %s", path, exc)
                    raise
                sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                logger.warning("Erro de rede. Dormindo %.1fs e tentando novamente...", sleep_s)
                time.sleep(sleep_s)
                continue

        return None

    def get_ticket(self, ticket_id: int, select_fields: str) -> Optional[Dict[str, Any]]:
        # tenta primeiro /tickets?id=...
        params = {
            "id": str(ticket_id),
            "includeDeletedItems": "true",
            "$select": select_fields,
        }
        logger.info("Buscando ticket %s em /tickets (id=...)", ticket_id)
        data = self._request("/tickets", params)

        if isinstance(data, list):
            data = data[0] if data else None

        if isinstance(data, dict) and data.get("id"):
            return data

        # fallback /tickets/past
        logger.info("Ticket %s não encontrado em /tickets; tentando /tickets/past", ticket_id)
        params_past = {
            "$filter": f"id eq {ticket_id}",
            "$select": select_fields,
            "includeDeletedItems": "true",
        }
        data_past = self._request("/tickets/past", params_past)

        if isinstance(data_past, list):
            data_past = data_past[0] if data_past else None

        if isinstance(data_past, dict) and data_past.get("id"):
            return data_past

        return None

    def list_tickets_after(self, last_id: int, limit: int, per_page: int, select_fields: str) -> List[Dict[str, Any]]:
        """
        Lista tickets Resolved/Closed/Canceled com id > last_id.
        Mantém $select reduzido (crítico para não estourar timeout).
        """
        results: List[Dict[str, Any]] = []
        remaining = max(limit, 0)
        current_last_id = last_id

        base_status_filter = (
            "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')"
        )

        while remaining > 0:
            top = min(remaining, per_page)
            params = {
                "$filter": f"id gt {current_last_id} and {base_status_filter}",
                "$orderby": "id",
                "$top": str(top),
                "$select": select_fields,
                "includeDeletedItems": "true",
            }

            logger.info("Listando até %s tickets em /tickets com id > %s (Resolved/Closed/Canceled)", top, current_last_id)
            data = self._request("/tickets", params)

            if not isinstance(data, list) or not data:
                break

            page_items: List[Dict[str, Any]] = []
            max_id_in_page = current_last_id

            for item in data:
                if not isinstance(item, dict) or "id" not in item:
                    continue
                try:
                    tid = int(item["id"])
                except Exception:
                    continue
                if tid <= current_last_id:
                    continue
                page_items.append(item)
                max_id_in_page = max(max_id_in_page, tid)

            if not page_items:
                break

            results.extend(page_items)
            remaining -= len(page_items)
            current_last_id = max_id_in_page

            if len(page_items) < top:
                break

        return results


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


def sync_new_tickets(
    conn,
    client: MovideskClient,
    limit: int,
    per_page: int,
    select_bulk: str,
) -> Tuple[int, int]:
    last_id = get_last_ticket_id(conn)
    logger.info("Último ticket_id em tickets_resolvidos_detail: %s", last_id)

    tickets = client.list_tickets_after(last_id, limit, per_page=per_page, select_fields=select_bulk)

    if not tickets:
        logger.info("Nenhum ticket novo (Resolved/Closed/Canceled) encontrado após id=%s.", last_id)
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

        logger.info("Processando ticket novo %s/%s (ID=%s)", idx, len(tickets), ticket_id)

        try:
            upsert_ticket_detail_json(conn, ticket_id, ticket)
            conn.commit()
            ok += 1
        except Exception as exc:
            logger.exception("Erro ao gravar ticket novo ticket_id=%s no Neon: %s", ticket_id, exc)
            conn.rollback()
            fail += 1

    return ok, fail


def sync_missing_tickets(
    conn,
    client: MovideskClient,
    limit: int,
    select_detail: str,
) -> Tuple[int, int]:
    pending_ids = fetch_pending_from_audit(conn, limit)

    if not pending_ids:
        logger.info("Nenhum ticket pendente na audit_recent_missing.")
        return 0, 0

    logger.info("%s tickets pendentes na fila audit_recent_missing (limite=%s).", len(pending_ids), limit)
    logger.info("Primeiros pendentes (até 5): %s", ", ".join(str(i) for i in pending_ids[:5]))

    ok = 0
    fail = 0

    for idx, ticket_id in enumerate(pending_ids, start=1):
        logger.info("Processando ticket pendente %s/%s (ID=%s)", idx, len(pending_ids), ticket_id)

        try:
            ticket = client.get_ticket(ticket_id, select_fields=select_detail)
        except Exception as exc:
            logger.exception("Erro inesperado ao buscar ticket_id=%s: %s", ticket_id, exc)
            conn.rollback()
            fail += 1
            continue

        if ticket is None:
            logger.warning("Ticket %s não encontrado em nenhum endpoint. Mantendo na fila.", ticket_id)
            fail += 1
            continue

        try:
            upsert_ticket_detail_json(conn, ticket_id, ticket)
            # opcional: remove da fila de missing
            try_remove_from_audit(conn, ticket_id)
            conn.commit()
            ok += 1
        except Exception as exc:
            logger.exception("Erro ao gravar ticket pendente ticket_id=%s no Neon: %s", ticket_id, exc)
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

    try:
        page_size = int(get_env("DETAIL_PAGE_SIZE", "50"))
    except ValueError:
        page_size = 50

    try:
        connect_timeout = int(get_env("MOVIDESK_CONNECT_TIMEOUT", "10"))
    except ValueError:
        connect_timeout = 10

    try:
        read_timeout = int(get_env("MOVIDESK_READ_TIMEOUT", "120"))
    except ValueError:
        read_timeout = 120

    try:
        max_attempts = int(get_env("DETAIL_MAX_ATTEMPTS", "6"))
    except ValueError:
        max_attempts = 6

    token = get_env("MOVIDESK_TOKEN", required=True)

    logger.info(
        "Iniciando sincronização detail (bulk=%s, missing=%s, page=%s, timeout=(%ss,%ss), attempts=%s).",
        bulk_limit,
        missing_limit,
        page_size,
        connect_timeout,
        read_timeout,
        max_attempts,
    )

    conn = get_db_connection()
    ensure_detail_table_exists(conn)

    # IMPORTANTE: $select reduzido para não estourar timeout
    # Mantém o que você usa nos cálculos: serviços, owner/ownerTeam, clients, statusHistories, customFieldValues etc.
    SELECT_BULK = (
        "id,protocol,type,subject,category,urgency,status,baseStatus,justification,origin,"
        "createdDate,isDeleted,owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
        "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,resolvedIn,closedIn,"
        "canceledIn,actionCount,reopenedIn,lastActionDate,lastUpdate,clients,statusHistories,"
        "customFieldValues,customFields,additionalFields,custom_fields"
    )

    # Para missing, usamos o mesmo select (pode ser o mesmo ou mais “completo” se você quiser)
    SELECT_DETAIL = SELECT_BULK

    client = MovideskClient(
        token=token,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_attempts=max_attempts,
    )

    ok_new, fail_new = sync_new_tickets(conn, client, bulk_limit, per_page=page_size, select_bulk=SELECT_BULK)
    ok_missing, fail_missing = sync_missing_tickets(conn, client, missing_limit, select_detail=SELECT_DETAIL)

    ok_total = ok_new + ok_missing
    fail_total = fail_new + fail_missing

    logger.info(
        "Processamento concluído. Sucesso=%s (novos=%s, missing=%s), Falhas=%s (novos=%s, missing=%s).",
        ok_total, ok_new, ok_missing, fail_total, fail_new, fail_missing
    )

    conn.close()


if __name__ == "__main__":
    main()
