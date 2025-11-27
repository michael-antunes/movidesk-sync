import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import requests

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("DATABASE_URL")

BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("DETAIL_THROTTLE", "0.25"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("detail")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "movidesk-sync/detail"})


def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN não definido no ambiente")
    url = path if path.startswith("http") else f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    p = dict(params or {})
    p["token"] = API_TOKEN
    r = SESSION.get(url, params=p, timeout=60)
    if r.status_code == 404 and ok_404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = SESSION.get(url, params=p, timeout=60)
        if r2.status_code == 404 and ok_404:
            return None
        r2.raise_for_status()
        return r2.json() or {}
    r.raise_for_status()
    return r.json() or {}


def get_pending_ids(conn, limit: int) -> List[int]:
    if limit is None or limit <= 0:
        limit = 200
    sql = """
        select ticket_id
          from visualizacao_resolvidos.audit_recent_missing
         where table_name = 'tickets_resolvidos'
         group by ticket_id
         order by max(run_id) desc, ticket_id desc
         limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    ids = [r[0] for r in rows]
    if ids:
        logger.info(
            "detail: %s tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
            len(ids),
            limit,
        )
    else:
        logger.info("detail: nenhum ticket pendente em visualizacao_resolvidos.audit_recent_missing.")
    return ids


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    if not ids:
        return
    sql = """
        delete from visualizacao_resolvidos.audit_recent_missing
         where table_name = 'tickets_resolvidos'
           and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    logger.warning("detail: ticket %s falhou: %s", ticket_id, reason)
    sql = """
        insert into visualizacao_resolvidos.audit_ticket_watch(table_name, ticket_id)
        values ('tickets_resolvidos', %s)
        on conflict (table_name, ticket_id) do nothing
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_id,))


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(text: Optional[str]) -> str:
    return (text or "").strip().lower()


def _find_times_from_actions(actions: List[Dict[str, Any]]) -> Dict[str, Optional[datetime]]:
    resolved = None
    closed = None
    cancelled = None
    for a in actions or []:
        status_txt = _norm(a.get("status"))
        created = (
            a.get("createdDate")
            or a.get("createdDateUtc")
            or a.get("createdAt")
            or a.get("date")
        )
        ts = _parse_dt(created)
        if not ts:
            continue
        if "resolvido" in status_txt or "resolved" in status_txt:
            if resolved is None or ts > resolved:
                resolved = ts
        if "fechado" in status_txt or "closed" in status_txt:
            if closed is None or ts > closed:
                closed = ts
        if "cancelado" in status_txt or "canceled" in status_txt:
            if cancelled is None or ts > cancelled:
                cancelled = ts
    return {
        "resolved": resolved,
        "closed": closed,
        "cancelled": cancelled,
    }


def _extract_csat(ticket: Dict[str, Any]) -> Optional[str]:
    candidates = []
    if isinstance(ticket.get("customFields"), list):
        candidates.extend(ticket["customFields"])
    if isinstance(ticket.get("customFieldValues"), list):
        candidates.extend(ticket["customFieldValues"])
    for cf in candidates:
        fid = cf.get("id") or cf.get("fieldId") or cf.get("customFieldId")
        name = _norm(cf.get("name") or cf.get("fieldName"))
        if fid == 137641 or "avaliado_csat" in name:
            val = cf.get("value") or cf.get("option") or cf.get("selectedChoice")
            if isinstance(val, dict):
                val = val.get("label") or val.get("id")
            return str(val) if val is not None else None
    for k, v in ticket.items():
        if _norm(k) == "adicional_137641_avaliado_csat":
            return str(v) if v is not None else None
    return None


def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    ticket_id = int(ticket["id"])
    status_txt = ticket.get("status") or ""
    status_norm = _norm(status_txt)
    actions = ticket.get("actions") or []
    times = _find_times_from_actions(actions)
    resolved_time = times["resolved"]
    closed_time = times["closed"]
    cancelled_time = times["cancelled"]
    last_resolved_at = None
    last_closed_at = None
    last_cancelled_at = None
    if "cancel" in status_norm:
        last_cancelled_at = cancelled_time or resolved_time or closed_time
    elif "resolvido" in status_norm or "resolved" in status_norm:
        last_resolved_at = resolved_time or cancelled_time
    elif "fechado" in status_norm or "closed" in status_norm:
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time
        last_cancelled_at = cancelled_time
    else:
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time
        last_cancelled_at = cancelled_time
    csat = _extract_csat(ticket)
    return {
        "ticket_id": ticket_id,
        "status": status_txt,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
        "last_cancelled_at": last_cancelled_at,
        "csat": csat,
    }


def upsert_details(conn, details: List[Dict[str, Any]]) -> None:
    if not details:
        return
    sql = """
        update visualizacao_resolvidos.tickets_resolvidos t
           set status                            = coalesce(%(status)s, t.status),
               last_resolved_at                  = coalesce(%(last_resolved_at)s, t.last_resolved_at),
               last_closed_at                    = coalesce(%(last_closed_at)s, t.last_closed_at),
               last_cancelled_at                 = coalesce(%(last_cancelled_at)s, t.last_cancelled_at),
               adicional_137641_avaliado_csat    = coalesce(%(csat)s, t.adicional_137641_avaliado_csat)
         where t.ticket_id = %(ticket_id)s
    """
    with conn.cursor() as cur:
        for row in details:
            cur.execute(sql, row)


def main() -> None:
    if not DSN:
        raise RuntimeError("NEON_DSN ou DATABASE_URL não definido no ambiente")
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)
        if not pending:
            logger.info("detail: nenhum ticket pendente para processar.")
            return
        logger.info(
            "detail: %s tickets pendentes para atualização de detalhes (limite=%s).",
            total_pendentes,
            BATCH_SIZE,
        )
        detalhes: List[Dict[str, Any]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}
        for tid in pending:
            reason = None
            try:
                data = md_get(
                    "tickets",
                    params={
                        "id": tid,
                        "$expand": "clients,createdBy,owner,actions,customFields,customFieldValues",
                    },
                    ok_404=True,
                )
            except requests.HTTPError as e:
                try:
                    status_code = e.response.status_code
                except Exception:
                    status_code = None
                reason = f"http_error_{status_code or 'unknown'}"
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
        logger.info(
            "detail: processados neste ciclo: ok=%s, falhas=%s.",
            total_ok,
            total_fail,
        )
        if fail_reasons:
            logger.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                logger.info("  - %s: %s tickets (exemplo ticket_id=%s)", r, c, sample)
        if not detalhes:
            conn.commit()
            logger.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch."
            )
            return
        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        conn.commit()
        logger.info(
            "detail: %s tickets upsertados em visualizacao_resolvidos.tickets_resolvidos e removidos de audit_recent_missing.",
            total_ok,
        )


if __name__ == "__main__":
    main()
