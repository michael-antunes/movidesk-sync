#!/usr/bin/env python3
import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2 import errors


# ------------------------------------------------------------------------------
# Configuração básica
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_resolved_detail")

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("POSTGRES_DSN")

BATCH_SIZE = int(os.getenv("DETAIL_BATCH_SIZE", "100"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.3"))  # segundos entre chamadas


# ------------------------------------------------------------------------------
# Util Movidesk
# ------------------------------------------------------------------------------

def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    """
    Chama a API do Movidesk com token e retorna JSON.
    Se ok_404=True, devolve None em caso de 404.
    """
    if params is None:
        params = {}

    qp = {"token": API_TOKEN}
    qp.update(params)

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.get(url, params=qp, timeout=30)

    if resp.status_code == 404 and ok_404:
        return None

    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------------------------------------
# Helpers de data / status
# ------------------------------------------------------------------------------

def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Movidesk normalmente manda ISO 8601, às vezes com "Z"
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


STATUS_RESOLVIDO = {"resolvido", "resolved"}
STATUS_FECHADO = {"fechado", "closed"}
STATUS_CANCELADO = {"cancelado", "cancelled", "canceled"}


def extract_status_times(ticket: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Percorre as ações do ticket e extrai:
      - last_resolved_at: última ação que colocou o ticket em status 'Resolvido'
      - last_closed_at:   última ação que colocou o ticket em 'Fechado' ou 'Cancelado'

    Para cancelados, usamos last_closed_at como “data de cancelamento”.
    """
    actions = ticket.get("actions") or []
    last_resolved_at: Optional[datetime] = None
    last_closed_at: Optional[datetime] = None

    for ac in actions:
        created_raw = (
            ac.get("createdDate")
            or ac.get("createdDateUtc")
            or ac.get("created_at")
        )
        created = parse_dt(created_raw)
        if not created:
            continue

        status_value = ac.get("status") or ac.get("statusName") or ""
        if isinstance(status_value, dict):
            status_value = (
                status_value.get("name")
                or status_value.get("description")
                or ""
            )

        status_str = str(status_value).strip().lower()

        # Resolvido
        if status_str in STATUS_RESOLVIDO:
            if not last_resolved_at or created > last_resolved_at:
                last_resolved_at = created

        # Fechado ou Cancelado -> usamos last_closed_at
        if status_str in STATUS_FECHADO or status_str in STATUS_CANCELADO:
            if not last_closed_at or created > last_closed_at:
                last_closed_at = created

    return last_resolved_at, last_closed_at


def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta apenas o que precisamos para esse job:
      - ticket_id
      - last_resolved_at
      - last_closed_at
    """
    tid = ticket.get("id")
    if tid is None:
        raise ValueError("ticket sem id")

    last_resolved_at, last_closed_at = extract_status_times(ticket)

    return {
        "ticket_id": int(tid),
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
    }


# ------------------------------------------------------------------------------
# Interação com o banco: missing, watch e update de tempos
# ------------------------------------------------------------------------------

def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Busca até `limit` ticket_ids em audit_recent_missing para os quais faltam
    os campos de tempo (last_resolved_at / last_closed_at).

    Tenta usar o schema novo (table_name/column_name). Se não existir coluna
    ou tabela, faz fallback pra versões mais simples ou retorna lista vazia.
    """
    sql_full = """
        WITH last_runs AS (
            SELECT
                m.ticket_id,
                MAX(r.run_at) AS last_run_at
            FROM audit_recent_missing m
            JOIN audit_recent_run r ON r.id = m.run_id
            WHERE m.table_name IN ('tickets_resolvidos', 'tickets_resolvidos_detail')
              AND m.column_name IN ('last_resolved_at', 'last_closed_at')
            GROUP BY m.ticket_id
        )
        SELECT ticket_id
        FROM last_runs
        ORDER BY last_run_at DESC, ticket_id DESC
        LIMIT %s
    """

    sql_fallback = """
        SELECT DISTINCT m.ticket_id
        FROM audit_recent_missing m
        JOIN audit_recent_run r ON r.id = m.run_id
        ORDER BY r.run_at DESC, m.ticket_id DESC
        LIMIT %s
    """

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_full, (limit,))
            except errors.UndefinedColumn:
                conn.rollback()
                logger.warning(
                    "detail: audit_recent_missing sem table_name/column_name; "
                    "usando consulta fallback."
                )
                with conn.cursor() as cur2:
                    cur2.execute(sql_fallback, (limit,))
                    rows = cur2.fetchall()
                    return [r[0] for r in rows]
            rows = cur.fetchall()
            return [r[0] for r in rows]

    except errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Sem tickets pendentes para processar."
        )
        return []


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    """
    Remove da audit_recent_missing os tickets que já foram atualizados com sucesso.

    Tenta usar filtro por table_name/column_name; se não existir coluna, apaga
    só por ticket_id; se não existir tabela, apenas loga erro.
    """
    if not ids:
        return

    sql_delete_full = """
        DELETE FROM audit_recent_missing
        WHERE ticket_id = ANY(%s)
          AND table_name IN ('tickets_resolvidos', 'tickets_resolvidos_detail')
          AND column_name IN ('last_resolved_at', 'last_closed_at')
    """

    sql_delete_simple = """
        DELETE FROM audit_recent_missing
        WHERE ticket_id = ANY(%s)
    """

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_delete_full, (ids,))
            except errors.UndefinedColumn:
                conn.rollback()
                logger.warning(
                    "detail: audit_recent_missing sem column_name/table_name; "
                    "apagando apenas por ticket_id."
                )
                with conn.cursor() as cur2:
                    cur2.execute(sql_delete_simple, (ids,))
        conn.commit()
        logger.info("DELETE MISSING: %d", len(ids))

    except errors.UndefinedTable:
        conn.rollback()
        logger.error(
            "detail: tabela audit_recent_missing não existe; nada foi deletado."
        )


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra falha em audit_ticket_watch (apenas ticket_id) e loga o motivo.

    Usa ON CONFLICT DO NOTHING pra não estourar duplicate key em runs repetidos.
    """
    logger.warning("detail: falha ao processar ticket %s: %s", ticket_id, reason)

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
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(
            "detail: erro ao registrar falha em audit_ticket_watch "
            "para ticket %s: %s",
            ticket_id,
            e,
        )


def upsert_details(conn, rows: List[Dict[str, Any]]) -> int:
    """
    Atualiza last_resolved_at e last_closed_at em tickets_resolvidos_detail.

    Só mexe nesses dois campos; o resto da linha permanece intacto.
    """
    if not rows:
        return 0

    updated = 0
    with conn.cursor() as cur:
        for r in rows:
            ticket_id = r["ticket_id"]
            last_resolved_at = r.get("last_resolved_at")
            last_closed_at = r.get("last_closed_at")

            cur.execute(
                """
                UPDATE tickets_resolvidos_detail
                SET last_resolved_at = COALESCE(%s, last_resolved_at),
                    last_closed_at  = COALESCE(%s, last_closed_at)
                WHERE ticket_id = %s
                """,
                (last_resolved_at, last_closed_at, ticket_id),
            )
            updated += cur.rowcount

    conn.commit()
    logger.info("UPSERT detail: %d linhas atualizadas.", updated)
    return updated


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> None:
    logger.info("Iniciando sync_resolved_detail (detail)...")

    if not DSN:
        logger.critical("DSN do banco não configurado (NEON_DSN/POSTGRES_DSN).")
        raise SystemExit(1)

    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)

        if not pending:
            logger.info("detail: nenhum ticket pendente em audit_recent_missing.")
            return

        logger.info(
            "detail: %d tickets pendentes em audit_recent_missing (limite=%d).",
            total_pendentes,
            BATCH_SIZE,
        )

        detalhes: List[Dict[str, Any]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid in pending:
            reason: Optional[str] = None
            ticket: Optional[Dict[str, Any]] = None

            # --- chamada API ---------------------------------------------------
            try:
                data = md_get(
                    f"tickets/{tid}",
                    params={"$expand": "clients,createdBy,owner,actions,customFields"},
                    ok_404=True,
                )
            except requests.HTTPError as e:
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                reason = f"http_error_{status or 'unknown'}"
            except Exception:
                reason = "exception_api"

            if reason:
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

            # --- monta linha de tempos ----------------------------------------
            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            detalhes.append(row)
            ok_ids.append(tid)

            # Respeitar throttle da API
            if THROTTLE > 0:
                time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = sum(fail_reasons.values())

        logger.info(
            "detail: processados neste ciclo: ok=%d, falhas=%d.",
            total_ok,
            total_fail,
        )

        if fail_reasons:
            logger.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                logger.info("  - %s: %d tickets (exemplo ticket_id=%s)", r, c, sample)

        if not detalhes:
            logger.info(
                "detail: nenhum ticket com detalhe válido; "
                "apenas falhas registradas em audit_ticket_watch."
            )
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        logger.info(
            "detail: %d tickets processados e removidos do missing.",
            total_ok,
        )


if __name__ == "__main__":
    main()
