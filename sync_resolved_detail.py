import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import psycopg2.errors
import requests

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("DATABASE_URL")

THROTTLE = float(os.getenv("DETAIL_THROTTLE", "0.2"))
BATCH = int(os.getenv("DETAIL_BATCH", "100"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("detail")


def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não definido no ambiente")

    base = API_BASE.rstrip("/")
    url = f"{base}/{path.lstrip('/')}"
    params = dict(params or {})
    params["token"] = API_TOKEN

    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 404 and ok_404:
        return None
    resp.raise_for_status()
    return resp.json()


def get_pending_ids_from_missing(conn, limit: int) -> List[int]:
    sql = """
        SELECT m.ticket_id
          FROM visualizacao_resolvidos.audit_recent_missing m
          JOIN visualizacao_resolvidos.audit_recent_run r ON r.id = m.run_id
         WHERE m.table_name = 'tickets_resolvidos'
         GROUP BY m.ticket_id
         ORDER BY max(r.run_at) DESC, m.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]


def get_pending_ids_from_tickets(conn, limit: int) -> List[int]:
    sql = """
        SELECT t.ticket_id
          FROM visualizacao_resolvidos.tickets_resolvidos t
         WHERE (
                t.last_resolved_at IS NULL
             OR t.last_closed_at IS NULL
             OR t.last_cancelled_at IS NULL
             OR t.adicional_137641_avaliado_csat IS NULL
         )
           AND t.status IN ('Resolvido', 'Fechado', 'Cancelado')
         ORDER BY t.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]


def get_pending_ids(conn, limit: int) -> List[int]:
    try:
        ids = get_pending_ids_from_missing(conn, limit)
        if ids:
            logger.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
                len(ids),
                limit,
            )
        else:
            logger.info(
                "detail: nenhum registro pendente em visualizacao_resolvidos.audit_recent_missing para tickets_resolvidos."
            )
        return ids
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_recent_missing ou audit_recent_run não existe neste banco. Caindo para tickets_resolvidos."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao consultar audit_recent_missing (%s). Caindo para tickets_resolvidos.",
            e,
        )

    try:
        ids = get_pending_ids_from_tickets(conn, limit)
        if ids:
            logger.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
                len(ids),
                limit,
            )
        else:
            logger.info(
                "detail: nenhum ticket pendente em visualizacao_resolvidos.tickets_resolvidos."
            )
        return ids
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela visualizacao_resolvidos.tickets_resolvidos não existe neste banco. Nada para processar."
        )
        return []
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao consultar visualizacao_resolvidos.tickets_resolvidos (%s). Nada para processar.",
            e,
        )
        return []


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    if not ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM visualizacao_resolvidos.audit_recent_missing
                 WHERE table_name = 'tickets_resolvidos'
                   AND ticket_id = ANY(%s)
                """,
                (ids,),
            )
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela visualizacao_resolvidos.audit_recent_missing não existe ao tentar limpar pendências."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao limpar visualizacao_resolvidos.audit_recent_missing (%s).",
            e,
        )


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    logger.warning("detail: ticket %s falhou: %s", ticket_id, reason)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO visualizacao_resolvidos.audit_ticket_watch (ticket_id, last_reason)
                VALUES (%s, %s)
                ON CONFLICT (ticket_id) DO UPDATE
                    SET last_reason = EXCLUDED.last_reason
                """,
                (ticket_id, reason),
            )
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela visualizacao_resolvidos.audit_ticket_watch não existe; falhas não serão registradas em tabela."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao registrar falha em visualizacao_resolvidos.audit_ticket_watch (%s).",
            e,
        )


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(text: Optional[str]) -> str:
    return (text or "").strip().lower()


def _find_last_times(actions) -> Dict[str, Optional[datetime]]:
    resolved = None
    closed = None
    cancelled = None
    for a in actions or []:
        status_txt = _norm(a.get("status") or a.get("statusTicket"))
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


def _extract_csat(ticket) -> Optional[str]:
    for cf in ticket.get("customFields") or []:
        fid = cf.get("id") or cf.get("fieldId") or cf.get("customFieldId")
        name = _norm(cf.get("name") or cf.get("fieldName"))
        if fid == 137641 or "avaliado_csat" in name:
            val = cf.get("value")
            if isinstance(val, dict):
                val = val.get("label") or val.get("id")
            return str(val) if val is not None else None

    for k, v in ticket.items():
        if _norm(k) == "adicional_137641_avaliado_csat":
            return str(v) if v is not None else None

    return None


def build_detail_row(ticket: dict) -> Dict[str, Any]:
    ticket_id = int(ticket["id"])
    final_status = _norm(ticket.get("status"))
    actions = ticket.get("actions") or []

    times = _find_last_times(actions)
    resolved_time = times["resolved"]
    closed_time = times["closed"]
    cancelled_time = times["cancelled"]

    last_resolved_at = None
    last_closed_at = None
    last_cancelled_at = None

    if "cancelado" in final_status or "canceled" in final_status:
        last_resolved_at = None
        last_closed_at = None
        last_cancelled_at = cancelled_time
    elif "resolvido" in final_status or "resolved" in final_status:
        last_resolved_at = resolved_time
        last_closed_at = None
        last_cancelled_at = None
    elif "fechado" in final_status or "closed" in final_status:
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time
        last_cancelled_at = None
    else:
        last_resolved_at = resolved_time
        last_closed_at = closed_time
        last_cancelled_at = cancelled_time

    csat = _extract_csat(ticket)

    return {
        "ticket_id": ticket_id,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
        "last_cancelled_at": last_cancelled_at,
        "csat": csat,
    }


def upsert_details(conn, details: List[Dict[str, Any]]) -> None:
    if not details:
        return
    try:
        with conn.cursor() as cur:
            for row in details:
                cur.execute(
                    """
                    UPDATE visualizacao_resolvidos.tickets_resolvidos AS t
                       SET last_resolved_at = COALESCE(%(last_resolved_at)s, t.last_resolved_at),
                           last_closed_at  = COALESCE(%(last_closed_at)s,  t.last_closed_at),
                           last_cancelled_at = COALESCE(%(last_cancelled_at)s, t.last_cancelled_at),
                           adicional_137641_avaliado_csat =
                               COALESCE(%(csat)s, t.adicional_137641_avaliado_csat)
                     WHERE t.ticket_id = %(ticket_id)s
                    """,
                    row,
                )
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela visualizacao_resolvidos.tickets_resolvidos não existe ao tentar atualizar detalhes."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao atualizar visualizacao_resolvidos.tickets_resolvidos (%s).",
            e,
        )


def main():
    if not DSN:
        raise RuntimeError("NEON_DSN ou DATABASE_URL não definido no ambiente")

    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("SET search_path TO visualizacao_resolvidos, public")
        except Exception as e:
            conn.rollback()
            logger.error("detail: erro ao definir search_path (%s).", e)

        pending = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            logger.info("detail: nenhum ticket pendente para processar.")
            return

        logger.info(
            "detail: %s tickets pendentes para atualização de detalhes (limite=%s).",
            total_pendentes,
            BATCH,
        )

        detalhes: List[Dict[str, Any]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid in pending:
            reason = None
            try:
                data = md_get(
                    f"tickets/{tid}",
                    params={"$expand": "clients,createdBy,owner,actions,customFields"},
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
                time.sleep(THROTTLE)
                continue

            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                time.sleep(THROTTLE)
                continue

            if isinstance(data, list):
                if not data:
                    reason = "empty_list"
                    register_ticket_failure(conn, tid, reason)
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    if reason not in fail_samples:
                        fail_samples[reason] = tid
                    time.sleep(THROTTLE)
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
            logger.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas."
            )
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)

        logger.info(
            "detail: %s tickets atualizados em visualizacao_resolvidos.tickets_resolvidos e removidos do missing (se existir).",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
