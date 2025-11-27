#!/usr/bin/env python
# sync_resolved_detail.py

import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import errors


API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("DATABASE_URL")

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))   # pausa entre chamadas na API
BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "100"))        # qtd máx de tickets por rodada

LOGGER = logging.getLogger("detail")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Helpers de API
# ---------------------------------------------------------------------------

def md_get(path: str,
           params: Optional[Dict[str, Any]] = None,
           ok_404: bool = False) -> Any:
    """Chama a API do Movidesk e retorna o JSON."""
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    final_params: Dict[str, Any] = {"token": API_TOKEN}
    if params:
        final_params.update(params)

    resp = requests.get(url, params=final_params, timeout=30)

    if ok_404 and resp.status_code == 404:
        return None

    resp.raise_for_status()
    return resp.json()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Converte string ISO do Movidesk em datetime (UTC)."""
    if not value:
        return None
    v = value.strip()
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        return datetime.fromisoformat(v)
    except Exception:
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return None


def latest_status_times(actions: List[Dict[str, Any]]) -> Dict[str, datetime]:
    """
    Retorna o último horário por status com base nas actions do ticket.
    Usa o createdDate de cada action.
    """
    result: Dict[str, datetime] = {}
    for a in actions or []:
        status = a.get("status")
        if not status:
            continue
        dt = parse_dt(a.get("createdDate") or a.get("createddate"))
        if not dt:
            continue
        prev = result.get(status)
        if prev is None or dt > prev:
            result[status] = dt
    return result


# ---------------------------------------------------------------------------
# Montagem de last_resolved_at / last_closed_at
# ---------------------------------------------------------------------------

def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """
    Regras:
      - Cancelado  -> last_resolved_at = data do cancelamento
      - Resolvido  -> last_resolved_at = data de resolução
      - Fechado    -> last_resolved_at = data de resolução, last_closed_at = fechamento
    """
    if "id" not in ticket:
        raise ValueError("ticket sem id")

    ticket_id = int(ticket["id"])
    status = (ticket.get("status") or "").strip()
    status_lower = status.lower()

    actions = ticket.get("actions") or []
    status_times = latest_status_times(actions)

    # Campos nativos do Movidesk
    resolved_in = parse_dt(ticket.get("resolvedIn"))
    closed_in = parse_dt(ticket.get("closedIn"))

    # Candidatos vindos das actions (backup)
    resolved_candidate = (
        resolved_in
        or status_times.get("Resolvido")
        or status_times.get("Resolved")
    )
    closed_candidate = (
        closed_in
        or status_times.get("Fechado")
        or status_times.get("Closed")
    )
    canceled_candidate = (
        status_times.get("Cancelado")
        or status_times.get("Canceled")
    )

    last_resolved_at: Optional[datetime] = None
    last_closed_at: Optional[datetime] = None

    if "cancel" in status_lower:  # Cancelado / Canceled
        last_resolved_at = canceled_candidate or resolved_candidate or closed_candidate
        last_closed_at = None

    elif "fechad" in status_lower or "closed" in status_lower:
        # Fechado: resolved = quando resolveu, closed = quando fechou
        last_resolved_at = resolved_candidate or canceled_candidate
        last_closed_at = closed_candidate

    elif "resolvid" in status_lower or "resolved" in status_lower:
        last_resolved_at = resolved_candidate or canceled_candidate
        last_closed_at = None

    else:
        # fallback bem genérico
        last_resolved_at = resolved_candidate or canceled_candidate
        last_closed_at = closed_candidate

    return {
        "ticket_id": ticket_id,
        "status": status,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
    }


# ---------------------------------------------------------------------------
# Busca de tickets pendentes
# ---------------------------------------------------------------------------

def get_pending_ids_from_missing(conn, limit: int) -> List[int]:
    """
    Busca em audit_recent_missing (quando existir) só colunas de data.
    """
    sql = """
        SELECT DISTINCT m.ticket_id
        FROM audit_recent_missing m
        WHERE m.column_name IN ('last_resolved_at', 'last_closed_at')
        ORDER BY m.ticket_id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_pending_ids_from_tickets(conn, limit: int) -> List[int]:
    """
    Fallback: busca diretamente na tabela tickets_resolvidos
    os tickets sem datas preenchidas.
    """
    sql = """
        SELECT t.ticket_id
        FROM tickets_resolvidos t
        WHERE
            (t.status = 'Resolvido'  AND t.last_resolved_at IS NULL)
         OR (t.status = 'Fechado'   AND (t.last_resolved_at IS NULL OR t.last_closed_at IS NULL))
         OR (t.status = 'Cancelado' AND t.last_resolved_at IS NULL)
        ORDER BY t.ticket_id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Tenta audit_recent_missing; se der erro, cai em tickets_resolvidos;
    se também der erro, retorna [] e não quebra o job.
    """
    # 1) Tenta pelo missing
    try:
        return get_pending_ids_from_missing(conn, limit)
    except errors.UndefinedTable:
        conn.rollback()
        LOGGER.error(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Caindo para busca direta em tickets_resolvidos."
        )
    except Exception as e:
        conn.rollback()
        LOGGER.error(
            "detail: erro ao buscar pendentes em audit_recent_missing (%s). "
            "Caindo para tickets_resolvidos.",
            e,
        )

    # 2) Fallback direto em tickets_resolvidos
    try:
        return get_pending_ids_from_tickets(conn, limit)
    except errors.UndefinedTable:
        conn.rollback()
        LOGGER.error(
            "detail: tabela tickets_resolvidos não existe neste banco. "
            "Nada para processar."
        )
        return []
    except Exception as e:
        conn.rollback()
        LOGGER.error(
            "detail: erro ao buscar pendentes em tickets_resolvidos (%s). "
            "Nada para processar.",
            e,
        )
        return []


# ---------------------------------------------------------------------------
# audit_ticket_watch (tolerante a esquema)
# ---------------------------------------------------------------------------

_AUDIT_TICKET_WATCH_COLUMNS: Optional[set] = None


def _ensure_audit_ticket_watch_columns(conn) -> set:
    """Descobre colunas disponíveis em audit_ticket_watch para montar o INSERT."""
    global _AUDIT_TICKET_WATCH_COLUMNS
    if _AUDIT_TICKET_WATCH_COLUMNS is not None:
        return _AUDIT_TICKET_WATCH_COLUMNS

    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'audit_ticket_watch'
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = {r[0] for r in cur.fetchall()}

    _AUDIT_TICKET_WATCH_COLUMNS = cols
    return cols


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra problemas no processamento do ticket em audit_ticket_watch.

    Se a tabela/colunas não existirem, só loga o erro e segue.
    """
    try:
        cols = _ensure_audit_ticket_watch_columns(conn)
    except Exception as e:
        conn.rollback()
        LOGGER.error(
            "detail: falha ao inspecionar audit_ticket_watch (%s). Ignorando watch.",
            e,
        )
        return

    with conn.cursor() as cur:
        try:
            if (
                "reason" in cols
                and "source" in cols
                and "failure_count" in cols
                and "last_failure_at" in cols
            ):
                cur.execute(
                    """
                    INSERT INTO audit_ticket_watch (ticket_id, source, reason, failure_count, last_failure_at)
                    VALUES (%s, %s, %s, 1, now())
                    ON CONFLICT (ticket_id) DO UPDATE
                      SET source = EXCLUDED.source,
                          reason = EXCLUDED.reason,
                          failure_count = audit_ticket_watch.failure_count + 1,
                          last_failure_at = now()
                    """,
                    (ticket_id, "detail", reason),
                )
            elif "reason" in cols and "source" in cols:
                cur.execute(
                    """
                    INSERT INTO audit_ticket_watch (ticket_id, source, reason)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ticket_id) DO UPDATE
                      SET source = EXCLUDED.source,
                          reason = EXCLUDED.reason
                    """,
                    (ticket_id, "detail", reason),
                )
            elif "reason" in cols:
                cur.execute(
                    """
                    INSERT INTO audit_ticket_watch (ticket_id, reason)
                    VALUES (%s, %s)
                    ON CONFLICT (ticket_id) DO UPDATE
                      SET reason = EXCLUDED.reason
                    """,
                    (ticket_id, reason),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO audit_ticket_watch (ticket_id)
                    VALUES (%s)
                    ON CONFLICT (ticket_id) DO NOTHING
                    """,
                    (ticket_id,),
                )
        except Exception as e:
            conn.rollback()
            LOGGER.error("detail: erro ao registrar falha em audit_ticket_watch (%s)", e)


def delete_processed_from_missing(conn, ticket_ids: List[int]) -> None:
    """Remove tickets processados de audit_recent_missing, se existir."""
    if not ticket_ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM audit_recent_missing WHERE ticket_id = ANY(%s)",
                (ticket_ids,),
            )
    except errors.UndefinedTable:
        conn.rollback()
        LOGGER.info("detail: tabela audit_recent_missing não existe; nada a remover.")
    except Exception as e:
        conn.rollback()
        LOGGER.error("detail: erro ao limpar audit_recent_missing (%s)", e)


# ---------------------------------------------------------------------------
# Persistência em tickets_resolvidos
# ---------------------------------------------------------------------------

def update_details(conn, rows: List[Dict[str, Any]]) -> None:
    """Atualiza os campos de data na tabela tickets_resolvidos."""
    if not rows:
        return

    sql = """
        UPDATE tickets_resolvidos
        SET
            status = %s,
            last_resolved_at = %s,
            last_closed_at = %s
        WHERE ticket_id = %s
    """
    params = [
        (
            row["status"],
            row["last_resolved_at"],
            row["last_closed_at"],
            row["ticket_id"],
        )
        for row in rows
    ]

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, params, page_size=100)
        LOGGER.info("detail: UPDATE detail: %s linhas atualizadas.", len(rows))
    except errors.UndefinedTable:
        conn.rollback()
        LOGGER.error(
            "detail: tabela tickets_resolvidos não existe; não foi possível atualizar."
        )
    except Exception as e:
        conn.rollback()
        LOGGER.error(
            "detail: erro ao atualizar tickets_resolvidos (%s).",
            e,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    LOGGER.info("detail: Iniciando sync_resolved_detail (detail)...")

    if not DSN:
        raise RuntimeError("NEON_DSN / DATABASE_URL não configurado")

    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)

        if not pending:
            LOGGER.info("detail: nenhum ticket pendente para processar.")
            return

        LOGGER.info(
            "detail: %s tickets pendentes (limite=%s).",
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
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue
            except Exception:
                reason = "exception_api"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

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

            # marca algumas inconsistências, mas sem travar o processamento
            if (
                row["status"] in ("Resolvido", "Fechado", "Cancelado")
                and not row["last_resolved_at"]
                and row["status"] != "Fechado"
            ):
                reason = "missing_resolved_for_status"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)

            if row["status"] == "Fechado" and not row["last_closed_at"]:
                reason = "missing_closed_for_status"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)

            detalhes.append(row)
            ok_ids.append(tid)
            time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = sum(fail_reasons.values())

        LOGGER.info("detail: processados neste ciclo: ok=%s, falhas=%s.", total_ok, total_fail)
        if fail_reasons:
            LOGGER.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                LOGGER.info(
                    "  - %s: %s tickets (exemplo ticket_id=%s)",
                    r,
                    c,
                    sample,
                )

        if not detalhes:
            LOGGER.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch."
            )
            return

        update_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        LOGGER.info(
            "detail: %s tickets atualizados em tickets_resolvidos e, se existir, "
            "removidos de audit_recent_missing.",
            total_ok,
        )


if __name__ == "__main__":
    main()
