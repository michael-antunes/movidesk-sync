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
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("detail")


def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    """Wrapper da API do Movidesk."""
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


def ensure_search_path(conn) -> None:
    """Garante que estamos usando o schema 'main'."""
    with conn.cursor() as cur:
        cur.execute("SET search_path TO main, public")


def get_pending_ids_from_missing(conn, limit: int) -> List[int]:
    """
    Busca tickets na audit_recent_missing que estão faltando
    last_resolved_at / last_closed_at / adicional_137641_avaliado_csat.
    """
    sql = """
        SELECT DISTINCT m.ticket_id
        FROM audit_recent_missing m
        WHERE m.table_name = 'tickets_resolvidos'
          AND m.column_name IN (
                'last_resolved_at',
                'last_closed_at',
                'adicional_137641_avaliado_csat'
          )
        ORDER BY m.ticket_id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_pending_ids(conn, limit: int) -> List[int]:
    """Wrapper com tratamento se a tabela não existir."""
    try:
        return get_pending_ids_from_missing(conn, limit)
    except psycopg2.errors.UndefinedTable:
        logger.error(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Sem tickets pendentes para processar."
        )
        conn.rollback()
        return []


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    """Remove da audit_recent_missing os tickets já processados."""
    if not ids:
        return

    sql = """
        DELETE FROM audit_recent_missing
        WHERE table_name = 'tickets_resolvidos'
          AND ticket_id = ANY(%s)
          AND column_name IN (
                'last_resolved_at',
                'last_closed_at',
                'adicional_137641_avaliado_csat'
          )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ids,))
    except psycopg2.errors.UndefinedTable:
        # Se a tabela não existir, apenas registra e segue.
        logger.error(
            "detail: tabela audit_recent_missing não existe ao tentar deletar processados."
        )
        conn.rollback()


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra falha em audit_ticket_watch.

    Para não quebrar por diferença de schema, só garantimos o INSERT do ticket_id;
    o motivo fica apenas no log.
    """
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
    except psycopg2.Error as e:
        logger.error(
            "detail: erro ao registrar falha do ticket %s em audit_ticket_watch: %s",
            ticket_id,
            e,
        )
        conn.rollback()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Movidesk costuma devolver ISO com timezone, que fromisoformat entende.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(text: Optional[str]) -> str:
    return (text or "").strip().lower()


def _find_last_action_time(actions: List[Dict[str, Any]], keywords: List[str]) -> Optional[datetime]:
    """
    Procura a última ação cujo status/descrição contenha algum dos keywords.
    """
    keywords = [k.lower() for k in keywords]
    last: Optional[datetime] = None

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


def _extract_csat(ticket: Dict[str, Any]) -> Optional[str]:
    """
    Tenta extrair o campo adicional_137641_avaliado_csat.

    Faz o melhor esforço a partir de customFields; se não achar, tenta olhar
    diretamente no dicionário do ticket (caso já venha flatten).
    """
    # 1) customFields (expandidos)
    for cf in ticket.get("customFields") or []:
        fid = cf.get("id") or cf.get("fieldId") or cf.get("customFieldId")
        name = _norm(cf.get("name") or cf.get("fieldName"))
        if fid == 137641 or "avaliado_csat" in name:
            val = cf.get("value")
            if isinstance(val, dict):
                # alguns campos vêm como {"id": "...", "label": "..."}
                val = val.get("label") or val.get("id")
            return str(val) if val is not None else None

    # 2) fallback: campo flatten no ticket
    for k, v in ticket.items():
        if _norm(k) == "adicional_137641_avaliado_csat":
            return str(v) if v is not None else None

    return None


def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula last_resolved_at / last_closed_at / csat para um ticket.
    """
    ticket_id = int(ticket["id"])
    status_txt = _norm(ticket.get("status"))
    actions = ticket.get("actions") or []

    resolved_time = _find_last_action_time(actions, ["resolvido"])
    closed_time = _find_last_action_time(actions, ["fechado"])
    cancelled_time = _find_last_action_time(actions, ["cancelado"])

    last_resolved_at: Optional[datetime] = None
    last_closed_at: Optional[datetime] = None

    if "fechado" in status_txt:
        # ticket fechado: tempo de resolução e de fechamento
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time
    elif "resolvido" in status_txt:
        # ticket resolvido, mas ainda não fechado
        last_resolved_at = resolved_time
        last_closed_at = None
    elif "cancelado" in status_txt:
        # cancelado normalmente não tem resolução/fechamento; usamos a data do cancelamento
        last_resolved_at = cancelled_time
        last_closed_at = None
    else:
        # fallback genérico
        last_resolved_at = resolved_time or cancelled_time
        last_closed_at = closed_time

    csat = _extract_csat(ticket)

    return {
        "ticket_id": ticket_id,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
        "csat": csat,
    }


def upsert_details(conn, details: List[Dict[str, Any]]) -> None:
    """
    Atualiza os campos de detalhe em tickets_resolvidos.

    Apenas faz UPDATE — assumimos que o ticket já existe na tabela,
    conforme o comportamento das triggers/auditoria.
    """
    if not details:
        return

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


def main() -> None:
    if not DSN:
        raise RuntimeError("NEON_DSN (ou DATABASE_URL) não definido no ambiente")

    with psycopg2.connect(DSN) as conn:
        ensure_search_path(conn)

        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)

        if not pending:
            logger.info("detail: nenhum ticket pendente em audit_recent_missing.")
            return

        logger.info(
            "detail: %s tickets pendentes em audit_recent_missing (limite=%s).",
            total_pendentes,
            BATCH_SIZE,
        )

        detalhes: List[Dict[str, Any]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid in pending:
            reason: Optional[str] = None

            try:
                data = md_get(
                    f"tickets/{tid}",
                    params={
                        "$expand": "clients,createdBy,owner,actions,customFields"
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
            except Exception as e:
                logger.exception(
                    "detail: erro ao montar row para ticket %s: %s", tid, e
                )
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
            "detail: %s tickets atualizados em tickets_resolvidos e removidos do missing.",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
