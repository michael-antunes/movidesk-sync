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


# ---------------------------
# Função genérica da API
# ---------------------------
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


# ---------------------------
# Pendências
# ---------------------------
def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Prioridade:
      1) audit_recent_missing (se existir e tiver dados)
      2) tickets_resolvidos (fallback)
    """
    # 1) tenta via audit_recent_missing
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ticket_id
                FROM audit_recent_missing
                WHERE table_name = 'tickets_resolvidos'
                  AND column_name IN (
                        'last_resolved_at',
                        'last_closed_at',
                        'adicional_137641_avaliado_csat'
                  )
                ORDER BY ticket_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        if rows:
            logger.info(
                "detail: %s tickets pendentes em audit_recent_missing (limite=%s).",
                len(rows),
                limit,
            )
            return [r[0] for r in rows]
        else:
            logger.info(
                "detail: nenhum registro pendente em audit_recent_missing para tickets_resolvidos."
            )
    except psycopg2.errors.UndefinedTable:
        logger.error(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Caindo para busca direta em tickets_resolvidos."
        )
    except Exception as e:
        logger.error(
            "detail: erro ao consultar audit_recent_missing (%s). "
            "Caindo para busca direta em tickets_resolvidos.",
            e,
        )

    # 2) fallback: tickets_resolvidos
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticket_id
                FROM tickets_resolvidos
                WHERE last_resolved_at IS NULL
                   OR (status = 'Fechado' AND last_closed_at IS NULL)
                   OR adicional_137641_avaliado_csat IS NULL
                ORDER BY ticket_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        if rows:
            logger.info(
                "detail: %s tickets pendentes em tickets_resolvidos (limite=%s).",
                len(rows),
                limit,
            )
            return [r[0] for r in rows]
        else:
            logger.info(
                "detail: nenhum ticket pendente em tickets_resolvidos para atualização de detalhes."
            )
    except psycopg2.errors.UndefinedTable:
        logger.error(
            "detail: tabela tickets_resolvidos não existe neste banco. Nada para processar."
        )
    except Exception as e:
        logger.error(
            "detail: erro ao consultar tickets_resolvidos (%s). Nada para processar.",
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
                        'adicional_137641_avaliado_csat'
                  )
                """,
                (ids,),
            )
    except psycopg2.errors.UndefinedTable:
        logger.error(
            "detail: tabela audit_recent_missing não existe ao tentar limpar pendências. "
            "Ignorando limpeza."
        )
    except Exception as e:
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
        logger.error(
            "detail: tabela audit_ticket_watch não existe; não será possível registrar falhas."
        )
    except Exception as e:
        logger.error(
            "detail: erro ao registrar falha em audit_ticket_watch (%s).", e
        )


# ---------------------------
# Helpers de parsing
# ---------------------------
def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(text: Optional[str]) -> str:
    return (text or "").strip().lower()


def _find_last_action_time(actions, keywords) -> Optional[datetime]:
    keywords = [k.lower() for k in keywords]
    last = None
    for a in actions or []:
        desc = _norm(a.get("description"))
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
        haystack = f"{desc} {status_txt}"
        if any(k in haystack for k in keywords):
            if last is None or ts > last:
                last = ts
    return last


def _extract_csat(ticket) -> Optional[str]:
    # customFields
    for cf in ticket.get("customFields") or []:
        fid = cf.get("id") or cf.get("fieldId") or cf.get("customFieldId")
        name = _norm(cf.get("name") or cf.get("fieldName"))
        if fid == 137641 or "avaliado_csat" in name:
            val = cf.get("value")
            if isinstance(val, dict):
                val = val.get("label") or val.get("id")
            return str(val) if val is not None else None

    # campo direto no ticket, se existir
    for k, v in ticket.items():
        if _norm(k) == "adicional_137641_avaliado_csat":
            return str(v) if v is not None else None

    return None


# ---------------------------
# Montagem do detalhe
# ---------------------------
def build_detail_row(ticket):
    ticket_id = int(ticket["id"])
    status_txt = _norm(ticket.get("status"))
    actions = ticket.get("actions") or []

    # pega os tempos olhando o histórico de ações
    resolved_time = _find_last_action_time(actions, ["resolvido"])
    closed_time = _find_last_action_time(actions, ["fechado"])
    cancelled_time = _find_last_action_time(actions, ["cancelado"])

    last_resolved_at = None
    last_closed_at = None

    if "fechado" in status_txt:
        # ticket atualmente Fechado → precisa ter resolvido + fechado
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time
    elif "resolvido" in status_txt:
        last_resolved_at = resolved_time
        last_closed_at = None
    elif "cancelado" in status_txt:
        last_resolved_at = cancelled_time
        last_closed_at = None
    else:
        # fallback pra algum caso estranho
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time

    csat = _extract_csat(ticket)

    return {
        "ticket_id": ticket_id,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
        "csat": csat,
    }


# ---------------------------
# Persistência em tickets_resolvidos
# ---------------------------
def upsert_details(conn, details):
    if not details:
        return
    try:
        with conn.cursor() as cur:
            for row in details:
                cur.execute(
                    """
                    UPDATE tickets_resolvidos
                       SET last_resolved_at = COALESCE(%(last_resolved_at)s, last_resolved_at),
                           last_closed_at  = COALESCE(%(last_closed_at)s,  last_closed_at),
                           adicional_137641_avaliado_csat =
                               COALESCE(%(csat)s, adicional_137641_avaliado_csat)
                     WHERE ticket_id = %(ticket_id)s
                    """,
                    row,
                )
    except psycopg2.errors.UndefinedTable:
        logger.error(
            "detail: tabela tickets_resolvidos não existe ao tentar atualizar detalhes."
        )
    except Exception as e:
        logger.error(
            "detail: erro ao atualizar tickets_resolvidos (%s).", e
        )


# ---------------------------
# Main
# ---------------------------
def main():
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
