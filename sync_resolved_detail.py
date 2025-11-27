import os
import time
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("PG_DSN")

BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not MOVIDESK_TOKEN:
    raise RuntimeError("Defina MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN")

if not DSN:
    raise RuntimeError("Defina NEON_DSN (ou PG_DSN) com a string de conexão do banco")

DETAIL_SELECT = ",".join(
    [
        "id",
        "subject",
        "type",
        "status",
        "baseStatus",
        "ownerTeam",
        "serviceFirstLevel",
        "serviceSecondLevel",
        "serviceThirdLevel",
        "origin",
        "category",
        "urgency",
        "createdDate",
        "lastUpdate",
    ]
)

DETAIL_EXPAND_FULL = "clients($expand=organization),createdBy,owner,actions,customFieldValues"
DETAIL_EXPAND_LIGHT = "clients($expand=organization),owner,actions"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] detail: %(message)s",
)
LOG = logging.getLogger("detail")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "movidesk-sync/detail"})


def md_get(
    path_or_full: str,
    params: Optional[Dict[str, Any]] = None,
    ok_404: bool = False,
):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = MOVIDESK_TOKEN

    while True:
        r = SESSION.get(url, params=p, timeout=60)

        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            try:
                wait = int(str(ra)) if ra is not None and str(ra).isdigit() else 60
            except Exception:
                wait = 60
            LOG.warning(
                "detail: HTTP %s na API, aguardando %s segundos antes de tentar novamente.",
                r.status_code,
                wait,
            )
            time.sleep(wait)
            continue

        if r.status_code == 404 and ok_404:
            return None

        if r.status_code >= 400:
            try:
                LOG.error(
                    "detail: HTTP %s em %s: %s",
                    r.status_code,
                    url,
                    r.text[:500],
                )
            except Exception:
                pass
            r.raise_for_status()

        return r.json() if r.text else {}


def _first_ticket_from_response(data: Any) -> Optional[Dict[str, Any]]:
    if isinstance(data, dict) and data.get("id"):
        return data
    if isinstance(data, list) and data:
        item = data[0]
        if isinstance(item, dict):
            return item
    return None


def fetch_ticket_with_fallback(tid: int) -> Optional[Dict[str, Any]]:
    params_base = {
        "$select": DETAIL_SELECT,
        "$filter": f"id eq {tid}",
        "$top": 1,
        "$skip": 0,
    }

    data = md_get(
        "tickets",
        params={**params_base, "$expand": DETAIL_EXPAND_FULL},
        ok_404=True,
    )
    ticket = _first_ticket_from_response(data)
    if ticket is not None:
        return ticket

    data = md_get(
        "tickets/past",
        params={**params_base, "$expand": DETAIL_EXPAND_FULL},
        ok_404=True,
    )
    ticket = _first_ticket_from_response(data)
    if ticket is not None:
        return ticket

    data = md_get(
        "tickets",
        params={**params_base, "$expand": DETAIL_EXPAND_LIGHT},
        ok_404=True,
    )
    ticket = _first_ticket_from_response(data)
    if ticket is not None:
        return ticket

    data = md_get(
        "tickets/past",
        params={**params_base, "$expand": DETAIL_EXPAND_LIGHT},
        ok_404=True,
    )
    ticket = _first_ticket_from_response(data)
    if ticket is not None:
        return ticket

    return None


SQL_GET_PENDING = """
SELECT ticket_id
  FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
 GROUP BY ticket_id
 ORDER BY max(run_id) DESC, ticket_id DESC
 LIMIT %s
"""


def get_pending_ids(conn, limit: int) -> List[int]:
    if not limit or limit <= 0:
        limit = BATCH

    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING, (limit,))
        rows = cur.fetchall()

    return [r[0] for r in rows]


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO visualizacao_resolvidos.audit_ticket_watch (
                    table_name, ticket_id, last_seen_at
                )
                VALUES ('tickets_resolvidos', %s, now())
                ON CONFLICT (ticket_id)
                DO UPDATE SET
                    table_name   = EXCLUDED.table_name,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (ticket_id,),
            )
        conn.commit()
    except Exception as e:
        LOG.error("detail: erro ao registrar falha em audit_ticket_watch (%s)", e)


def delete_from_missing(conn, ids: List[int]) -> None:
    if not ids:
        return

    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
         WHERE table_name = 'tickets_resolvidos'
           AND ticket_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()


def parse_movidesk_datetime(value: Optional[str]):
    if not value:
        return None
    try:
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def build_detail_row(ticket: Dict[str, Any]):
    actions = ticket.get("actions") or []

    last_resolved_at = None
    last_closed_at = None
    canceled_at = None

    for a in actions:
        status = (a.get("status") or "").strip()
        created = a.get("createdDate")
        if not created:
            continue

        dt = parse_movidesk_datetime(created)
        if dt is None:
            continue

        if status in ("Resolvido", "Resolved"):
            last_resolved_at = dt

        if status in ("Fechado", "Closed"):
            last_closed_at = dt

        if status in ("Cancelado", "Canceled"):
            canceled_at = dt

    final_status = (ticket.get("status") or "").strip()

    if final_status == "Cancelado":
        last_resolved_at_db = None
        last_closed_at_db = None
    elif final_status == "Resolvido":
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = None
    elif final_status == "Fechado":
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = last_closed_at
    else:
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = last_closed_at

    owner = ticket.get("owner") or {}
    org = ticket.get("organization") or {}
    clients = ticket.get("clients") or []
    client = clients[0] if clients else {}
    if not org and isinstance(client, dict):
        org = client.get("organization") or {}

    return (
        int(ticket["id"]),
        final_status or None,
        last_resolved_at_db,
        last_closed_at_db,
        canceled_at,
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


def upsert_details(conn, rows: List[tuple]) -> None:
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


def main():
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            LOG.info("detail: nenhum ticket pendente em audit_recent_missing/tickets_resolvidos.")
            return

        LOG.info(
            "detail: %d tickets pendentes para atualização de detalhes (limite=%d).",
            total_pendentes,
            BATCH,
        )

        detalhes: List[tuple] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid in pending:
            reason = None

            try:
                data = fetch_ticket_with_fallback(tid)
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                reason = f"http_error_{status or 'unknown'}"
            except Exception:
                reason = "exception_api"

            if reason is not None:
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if isinstance(data, list):
                if not data:
                    reason = "empty_list"
                    register_ticket_failure(conn, tid, reason)
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    if reason not in fail_samples:
                        fail_samples[reason] = tid
                    continue
                ticket = data[0]
            else:
                ticket = data

            if not ticket.get("id"):
                reason = "missing_id"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            detalhes.append(row)
            ok_ids.append(tid)
            time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = sum(fail_reasons.values())
        LOG.info(
            "detail: processados neste ciclo: ok=%d, falhas=%d.",
            total_ok,
            total_fail,
        )

        if fail_reasons:
            LOG.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                LOG.info(
                    "detail:   - %s: %d tickets (exemplo ticket_id=%s)",
                    r,
                    c,
                    sample,
                )

        if not detalhes:
            LOG.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch."
            )
            delete_from_missing(conn, pending)
            LOG.info(
                "detail: %d tickets removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
                len(pending),
            )
            return

        upsert_details(conn, detalhes)
        delete_from_missing(conn, pending)
        LOG.info(
            "detail: %d tickets upsertados em visualizacao_resolvidos.tickets_resolvidos "
            "e removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
