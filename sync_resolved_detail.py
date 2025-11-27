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
BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "100"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("detail")


def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não definido no ambiente")

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    params = dict(params or {})
    params["token"] = API_TOKEN

    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 404 and ok_404:
        return None
    resp.raise_for_status()
    return resp.json()


def get_pending_ids_from_missing(conn, limit: int) -> List[int]:
    sql = """
        SELECT DISTINCT m.ticket_id
          FROM audit_recent_missing m
          JOIN audit_recent_run r ON r.id = m.run_id
         WHERE m.table_name = 'tickets_resolvidos'
           AND m.column_name IN (
                'last_resolved_at',
                'last_closed_at',
                'last_cancelled_at',
                'adicional_137641_avaliado_csat'
           )
         ORDER BY r.run_at DESC, m.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    if rows:
        logger.info(
            "detail: %s tickets pendentes em audit_recent_missing (limite=%s).",
            len(rows),
            limit,
        )
    else:
        logger.info(
            "detail: nenhum registro pendente em audit_recent_missing para tickets_resolvidos."
        )

    return [r[0] for r in rows]


def get_pending_ids_from_tickets(conn, limit: int) -> List[int]:
    sql = """
        SELECT t.ticket_id
          FROM visualizacao_resolvidos.tickets_resolvidos t
         WHERE (
                  t.last_resolved_at IS NULL
               OR (t.status = 'Fechado'   AND t.last_closed_at    IS NULL)
               OR (t.status = 'Cancelado' AND t.last_cancelled_at IS NULL)
               OR t.adicional_137641_avaliado_csat IS NULL
         )
           AND t.status IN ('Resolvido', 'Fechado', 'Cancelado')
         ORDER BY t.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    if rows:
        logger.info(
            "detail: %s tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
            len(rows),
            limit,
        )
    else:
        logger.info(
            "detail: nenhum ticket pendente em visualizacao_resolvidos.tickets_resolvidos para atualização de detalhes."
        )

    return [r[0] for r in rows]


def get_pending_ids(conn, limit: int) -> List[int]:
    try:
        ids = get_pending_ids_from_missing(conn, limit)
        if ids:
            return ids
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Caindo para busca direta em tickets_resolvidos."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao consultar audit_recent_missing (%s). "
            "Caindo para busca direta em tickets_resolvidos.",
            e,
        )

    try:
        return get_pending_ids_from_tickets(conn, limit)
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
                DELETE FROM audit_recent_missing
                WHERE table_name = 'tickets_resolvidos'
                  AND ticket_id = ANY(%s)
                  AND column_name IN (
                        'last_resolved_at',
                        'last_closed_at',
                        'last_cancelled_at',
                        'adicional_137641_avaliado_csat'
                  )
                """,
                (ids,),
            )
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_recent_missing não existe ao tentar limpar pendências. "
            "Ignorando limpeza."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao limpar audit_recent_missing (%s).", e
        )


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    logger.warning("detail: ticket %s falhou: %s", ticket_id, reason)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_ticket_watch (ticket_id)
                VALUES (%s)
                ON CONFLICT (ticket_id) DO NOTHING
                """,
                (ticket_id,),
            )
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_ticket_watch não existe; não será possível registrar falhas."
        )
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao registrar falha em audit_ticket_watch (%s).", e
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


def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    ticket_id = int(ticket["id"])
    actions = ticket.get("actions") or []

    last_resolved_at = None
    last_closed_at = None
    last_cancelled_at = None

    for a in actions:
        status_txt = (a.get("status") or "").strip()
        created = (
            a.get("createdDate")
            or a.get("createdDateUtc")
            or a.get("createdAt")
            or a.get("date")
        )
        if not created:
            continue
        dt = _parse_dt(created)
        if not dt:
            continue

        status_norm = status_txt.strip().lower()

        if status_norm in ("resolvido", "resolved"):
            if last_resolved_at is None or dt > last_resolved_at:
                last_resolved_at = dt
        elif status_norm in ("fechado", "closed"):
            if last_closed_at is None or dt > last_closed_at:
                last_closed_at = dt
        elif status_norm in ("cancelado", "canceled"):
            if last_cancelled_at is None or dt > last_cancelled_at:
                last_cancelled_at = dt

    final_status = _norm(ticket.get("status"))

    if final_status == "cancelado":
        last_resolved_at_db = None
        last_closed_at_db = None
    elif final_status == "resolvido":
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = None
    elif final_status == "fechado":
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = last_closed_at
    else:
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = last_closed_at

    csat = _extract_csat(ticket)

    return {
        "ticket_id": ticket_id,
        "last_resolved_at": last_resolved_at_db,
        "last_closed_at": last_closed_at_db,
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
                    UPDATE visualizacao_resolvidos.tickets_resolvidos
                       SET last_resolved_at = COALESCE(%(last_resolved_at)s, last_resolved_at),
                           last_closed_at  = COALESCE(%(last_closed_at)s,  last_closed_at),
                           last_cancelled_at = COALESCE(%(last_cancelled_at)s, last_cancelled_at),
                           adicional_137641_avaliado_csat =
                               COALESCE(%(csat)s, adicional_137641_avaliado_csat)
                     WHERE ticket_id = %(ticket_id)s
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
        conn.autocommit = True

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
            logger.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas."
            )
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)

        logger.info(
            "detail: %s tickets atualizados em tickets_resolvidos e removidos do missing (se existir).",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
