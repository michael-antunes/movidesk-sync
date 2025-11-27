import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("PG_DSN")

BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))
MAX_429_RETRIES = int(os.getenv("MAX_429_RETRIES", "1"))

if not MOVIDESK_TOKEN:
    raise RuntimeError("Defina MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN")

if not DSN:
    raise RuntimeError("Defina NEON_DSN (ou PG_DSN) com a string de conexão do banco")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] detail: %(message)s",
)
LOG = logging.getLogger("detail")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "movidesk-sync/detail"})

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


def _parse_retry_after(headers: Dict[str, str]) -> Optional[int]:
    val = headers.get("Retry-After") or headers.get("retry-after")
    if not val:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def md_get(
    path_or_full: str,
    params: Optional[Dict[str, Any]] = None,
    ok_404: bool = False,
) -> Any:
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    base_params = dict(params or {})
    base_params["token"] = MOVIDESK_TOKEN

    retries = 0

    while True:
        resp = SESSION.get(url, params=base_params, timeout=60)

        if resp.status_code == 200:
            return resp.json() or {}

        if ok_404 and resp.status_code == 404:
            return None

        if resp.status_code == 429:
            retries += 1
            wait = _parse_retry_after(resp.headers) or 60
            LOG.warning(
                "detail: HTTP 429 em %s (tentativa %d/%d, Retry-After=%s). Aguardando %s segundos...",
                url,
                retries,
                MAX_429_RETRIES,
                resp.headers.get("Retry-After"),
                wait,
            )
            if retries >= MAX_429_RETRIES:
                resp.raise_for_status()
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503, 504):
            retries += 1
            backoff = 2 * retries
            LOG.warning(
                "detail: HTTP %s em %s (tentativa %d/%d). Aguardando %s segundos...",
                resp.status_code,
                url,
                retries,
                MAX_429_RETRIES,
                backoff,
            )
            if retries >= MAX_429_RETRIES:
                resp.raise_for_status()
            time.sleep(backoff)
            continue

        try:
            LOG.error(
                "detail: HTTP %s em %s: %s",
                resp.status_code,
                url,
                resp.text[:500],
            )
        except Exception:
            pass
        resp.raise_for_status()


SQL_GET_PENDING = """
SELECT arm.ticket_id,
       COALESCE(tr.last_resolved_at, tr.last_cancelled_at, tr.last_closed_at, tr.last_update) AS ref_date
  FROM visualizacao_resolvidos.audit_recent_missing AS arm
  LEFT JOIN visualizacao_resolvidos.tickets_resolvidos AS tr
         ON tr.ticket_id = arm.ticket_id
 WHERE arm.table_name = 'tickets_resolvidos'
 GROUP BY arm.ticket_id, ref_date
 ORDER BY MAX(arm.run_id) DESC, arm.ticket_id DESC
 LIMIT %s
"""


def get_pending_with_refdate(conn, limit: int) -> List[Tuple[int, Optional[datetime]]]:
    if not limit or limit <= 0:
        limit = BATCH

    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING, (limit,))
        rows = cur.fetchall()

    result: List[Tuple[int, Optional[datetime]]] = []
    for tid, ref_date in rows:
        result.append((int(tid), ref_date))
    return result


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


def parse_movidesk_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def build_detail_row(ticket: Dict[str, Any]) -> Tuple[Any, ...]:
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


def upsert_details(conn, rows: List[Tuple[Any, ...]]) -> None:
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


def _first_ticket_from_response(data: Any) -> Optional[Dict[str, Any]]:
    if isinstance(data, dict) and data.get("id"):
        return data
    if isinstance(data, list) and data:
        item = data[0]
        if isinstance(item, dict):
            return item
    return None


def _should_use_past_first(ref_date: Optional[datetime]) -> bool:
    if ref_date is None:
        return False
    now = datetime.utcnow()
    return ref_date < now - timedelta(days=90)


def fetch_ticket_with_fallback(
    ticket_id: int,
    ref_date: Optional[datetime],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    last_reason: Optional[str] = None

    params_base = {
        "$filter": f"id eq {ticket_id}",
        "$select": DETAIL_SELECT,
        "$top": 1,
        "$skip": 0,
    }

    prefer_past = _should_use_past_first(ref_date)
    first_endpoint = "tickets/past" if prefer_past else "tickets"
    second_endpoint = "tickets" if prefer_past else "tickets/past"

    for endpoint in (first_endpoint, second_endpoint):
        try:
            data = md_get(
                endpoint,
                params={**params_base, "$expand": DETAIL_EXPAND_FULL},
                ok_404=True,
            )
            ticket = _first_ticket_from_response(data)
            if ticket is not None:
                return ticket, None
            last_reason = "not_found_404"
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 404:
                last_reason = "not_found_404"
            elif status == 429:
                last_reason = "http_error_429"
            elif status == 400:
                last_reason = "http_error_400"
            else:
                last_reason = f"http_error_{status or 'unknown'}"
        except Exception:
            last_reason = "exception_api"

    return None, last_reason or "unknown"


def main() -> None:
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_with_refdate(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            LOG.info(
                "detail: nenhum ticket pendente em visualizacao_resolvidos.audit_recent_missing/tickets_resolvidos."
            )
            return

        LOG.info(
            "detail: %d tickets pendentes para atualização de detalhes (limite=%d).",
            total_pendentes,
            BATCH,
        )

        detalhes: List[Tuple[Any, ...]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid, ref_date in pending:
            ticket, reason = fetch_ticket_with_fallback(tid, ref_date)

            if ticket is None:
                reason = reason or "unknown"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                time.sleep(THROTTLE)
                continue

            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                time.sleep(THROTTLE)
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
            delete_from_missing(conn, [tid for (tid, _d) in pending])
            LOG.info(
                "detail: %d tickets removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
                len(pending),
            )
            return

        upsert_details(conn, detalhes)
        delete_from_missing(conn, [tid for (tid, _d) in pending])
        LOG.info(
            "detail: %d tickets upsertados em visualizacao_resolvidos.tickets_resolvidos "
            "e removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
