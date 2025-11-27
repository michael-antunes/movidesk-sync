#!/usr/bin/env python
import os
import time
import logging

import requests
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------------------

API_BASE = "https://api.movidesk.com/public/v1"

TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] detail: %(message)s",
)

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})


# ---------------------------------------------------------------------
# Funções de apoio – API Movidesk
# ---------------------------------------------------------------------
def md_get(path_or_full: str, params=None, ok_404: bool = False):
    """
    Chamada simples para a API do Movidesk.
    Se ok_404=True, retorna None em caso de HTTP 404.
    Faz uma segunda tentativa para erros 5xx / 429.
    """
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN

    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or {}

    if ok_404 and r.status_code == 404:
        return None

    # Tenta novamente em casos transitórios
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or {}
        if ok_404 and r2.status_code == 404:
            return None
        r2.raise_for_status()

    r.raise_for_status()


# ---------------------------------------------------------------------
# SQLs
# ---------------------------------------------------------------------

# 1) Leitura prioritária no audit_recent_missing
SQL_GET_PENDING_FROM_MISSING = """
SELECT DISTINCT ON (ticket_id)
       ticket_id,
       run_id
  FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
 ORDER BY ticket_id DESC, run_id DESC
 LIMIT %s
"""

# 2) Fallback: linhas em tickets_resolvidos sem last_update
SQL_GET_PENDING_FROM_TICKETS = """
SELECT ticket_id
  FROM visualizacao_resolvidos.tickets_resolvidos
 WHERE last_update IS NULL
 ORDER BY ticket_id DESC
 LIMIT %s
"""

SQL_DELETE_MISSING = """
DELETE FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
   AND ticket_id = ANY(%s)
"""

SQL_UPSERT_FAILURE_WATCH = """
INSERT INTO visualizacao_resolvidos.audit_ticket_watch (ticket_id, table_name, last_seen_at)
VALUES (%s, 'tickets_resolvidos', now())
ON CONFLICT (ticket_id) DO UPDATE
   SET last_seen_at = EXCLUDED.last_seen_at,
       table_name   = EXCLUDED.table_name
"""


# ---------------------------------------------------------------------
# Funções de banco
# ---------------------------------------------------------------------
def register_ticket_failure(conn, ticket_id: int, reason: str):
    """
    Registra que um ticket falhou ao buscar detalhe.
    Usa UPSERT para não estourar PK se já existir.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_UPSERT_FAILURE_WATCH, (ticket_id,))
        conn.commit()
    except Exception as e:
        logging.error(
            "detail: erro ao registrar falha em audit_ticket_watch (ticket_id=%s, reason=%s, err=%s)",
            ticket_id,
            reason,
            e,
        )


def get_pending_ids(conn, limit_: int):
    """
    Prioriza IDs do audit_recent_missing.
    Se der erro ou não houver nada, cai para tickets_resolvidos (backfill).
    """
    if not limit_ or limit_ <= 0:
        limit_ = 200

    # 1) Tenta pelo audit_recent_missing
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_GET_PENDING_FROM_MISSING, (limit_,))
            rows = cur.fetchall()
        ids = [r[0] for r in rows]
        if ids:
            logging.info(
                "detail: %d tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
                len(ids),
                limit_,
            )
            return ids
    except Exception as e:
        logging.error(
            "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s). "
            "Caindo para tickets_resolvidos.",
            e,
        )

    # 2) Fallback – tickets_resolvidos sem last_update
    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING_FROM_TICKETS, (limit_,))
        rows = cur.fetchall()
    ids = [r[0] for r in rows]
    if ids:
        logging.info(
            "detail: %d tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
            len(ids),
            limit_,
        )
    else:
        logging.info(
            "detail: nenhum ticket pendente em audit_recent_missing nem em tickets_resolvidos."
        )
    return ids


def delete_from_missing(conn, ids):
    """
    Remove do audit_recent_missing todos os IDs processados
    (tanto ok quanto falha).
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_MISSING, (ids,))
    conn.commit()


# ---------------------------------------------------------------------
# Montagem da linha para o INSERT
# ---------------------------------------------------------------------
def build_detail_row(ticket: dict):
    owner = ticket.get("owner") or {}
    org = ticket.get("organization") or {}
    clients = ticket.get("clients") or []
    client = clients[0] if clients else {}

    return (
        int(ticket["id"]),
        ticket.get("status"),
        ticket.get("lastResolvedDate"),
        ticket.get("lastClosedDate"),
        ticket.get("lastCancelledDate"),
        ticket.get("lastUpdate"),
        ticket.get("origin"),
        ticket.get("category"),
        ticket.get("urgency"),
        ticket.get("serviceFirstLevel"),
        ticket.get("serviceSecondLevel"),
        ticket.get("serviceThirdLevel"),
        owner.get("id"),
        owner.get("businessName"),
        owner.get("team"),
        org.get("id"),
        org.get("businessName"),
        ticket.get("subject"),
        client.get("businessName"),
    )


def upsert_details(conn, rows):
    """
    Upsert em visualizacao_resolvidos.tickets_resolvidos.
    A coluna adicionado_em_tabela fica por conta do DEFAULT do banco.
    """
    if not rows:
        return

    sql = """
    INSERT INTO visualizacao_resolvidos.tickets_resolvidos (
      ticket_id,
      status,
      last_resolved_at,
      last_closed_at,
      last_cancelled_at,
      last_update,
      origin,
      category,
      urgency,
      service_first_level,
      service_second_level,
      service_third_level,
      owner_id,
      owner_name,
      owner_team_name,
      organization_id,
      organization_name,
      subject,
      adicional_nome
    )
    VALUES %s
    ON CONFLICT (ticket_id) DO UPDATE SET
      status               = EXCLUDED.status,
      last_resolved_at     = EXCLUDED.last_resolved_at,
      last_closed_at       = EXCLUDED.last_closed_at,
      last_cancelled_at    = EXCLUDED.last_cancelled_at,
      last_update          = EXCLUDED.last_update,
      origin               = EXCLUDED.origin,
      category             = EXCLUDED.category,
      urgency              = EXCLUDED.urgency,
      service_first_level  = EXCLUDED.service_first_level,
      service_second_level = EXCLUDED.service_second_level,
      service_third_level  = EXCLUDED.service_third_level,
      owner_id             = EXCLUDED.owner_id,
      owner_name           = EXCLUDED.owner_name,
      owner_team_name      = EXCLUDED.owner_team_name,
      organization_id      = EXCLUDED.organization_id,
      organization_name    = EXCLUDED.organization_name,
      subject              = EXCLUDED.subject,
      adicional_nome       = EXCLUDED.adicional_nome
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            logging.info(
                "detail: nenhum ticket pendente para atualização de detalhes (limite=%s).",
                BATCH,
            )
            return

        logging.info(
            "detail: %d tickets pendentes para atualização de detalhes (limite=%s).",
            total_pendentes,
            BATCH,
        )

        detalhes = []
        fail_reasons = {}
        fail_samples = {}

        for tid in pending:
            reason = None

            # -----------------------------------------------------------------
            # Chamada da API do Movidesk
            # -----------------------------------------------------------------
            try:
                data = md_get(
                    f"tickets/{tid}",
                    params={"$expand": "clients,createdBy,owner,actions,customFields"},
                    ok_404=True,
                )
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                reason = f"http_error_{status or 'unknown'}"
            except Exception:
                reason = "exception_api"

            if reason is not None:
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            # 404 “de verdade”
            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            # Resposta em lista (caso raro)
            if isinstance(data, list):
                if not data:
                    reason = "empty_list"
                    register_ticket_failure(conn, tid, reason)
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    fail_samples.setdefault(reason, tid)
                    continue
                ticket = data[0]
            else:
                ticket = data

            if not ticket.get("id"):
                reason = "missing_id"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            detalhes.append(row)
            time.sleep(THROTTLE)

        total_ok = len(detalhes)
        total_fail = total_pendentes - total_ok

        logging.info(
            "detail: processados neste ciclo: ok=%d, falhas=%d.",
            total_ok,
            total_fail,
        )

        if fail_reasons:
            logging.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                logging.info(
                    "detail:   - %s: %d tickets (exemplo ticket_id=%s)",
                    r,
                    c,
                    sample,
                )

        if not detalhes:
            logging.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas."
            )
        else:
            upsert_details(conn, detalhes)
            logging.info(
                "detail: %d tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.",
                total_ok,
            )

        # Remove todos (ok + falha) do audit_recent_missing
        delete_from_missing(conn, pending)
        logging.info(
            "detail: %d tickets removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
            total_pendentes,
        )


if __name__ == "__main__":
    main()
