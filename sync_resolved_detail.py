import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("detail")

BASE_URL = "https://api.movidesk.com/public/v1"
API_TOKEN = os.environ.get("MOVIDESK_TOKEN") or os.environ.get("MOVIDESK_API_TOKEN")
DSN = os.environ.get("NEON_DSN")
PAGE_LIMIT = int(os.environ.get("PAGES_UPSERT", "50"))
REQUEST_TIMEOUT = 20


def _should_use_past_first(ref_date: Optional[datetime]) -> bool:
    """Decide se deve tentar /tickets/past primeiro (tickets com mais de 90 dias)."""
    if ref_date is None:
        return False

    if ref_date.tzinfo is None:
        ref_date = ref_date.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    return ref_date < now - timedelta(days=90)


def _fetch_ticket_once(
    ticket_id: int, endpoint: str, ref_date: Optional[datetime]
) -> Tuple[Optional[Dict[str, Any]], str, Dict[str, Any]]:
    """
    Faz uma chamada única para um endpoint (tickets ou tickets/past).

    Retorna (ticket_dict_ou_None, reason, meta)
    - reason: "ok", "not_found", "http_error_400", "timeout", etc.
    - meta: infos extras (endpoint, status_code, body_snippet)
    """
    if endpoint not in ("tickets", "tickets/past"):
        raise ValueError(f"endpoint inválido: {endpoint}")

    if endpoint == "tickets/past" and ref_date is None:
        # Evita chamar /tickets/past sem ter uma referência de data
        logger.info(
            "detail: ignorando endpoint=tickets/past para ticket_id=%s porque ref_date é None",
            ticket_id,
        )
        return None, "past_without_ref_date", {
            "endpoint": endpoint,
            "status_code": None,
            "body": None,
        }

    url = f"{BASE_URL}/{endpoint}/{ticket_id}"
    logger.info("detail: consultando endpoint=%s para ticket_id=%s", endpoint, ticket_id)

    params = {"token": API_TOKEN}

    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    except requests.Timeout:
        logger.warning(
            "detail: timeout ao chamar endpoint=%s para ticket_id=%s",
            endpoint,
            ticket_id,
        )
        return None, "timeout", {
            "endpoint": endpoint,
            "status_code": None,
            "body": None,
        }
    except requests.RequestException as exc:
        logger.warning(
            "detail: erro de request ao chamar endpoint=%s para ticket_id=%s: %s",
            endpoint,
            ticket_id,
            exc,
        )
        return None, "request_exception", {
            "endpoint": endpoint,
            "status_code": None,
            "body": str(exc),
        }

    status = resp.status_code
    body_snippet = resp.text[:500] if status != 200 else ""

    if status == 200:
        try:
            data = resp.json()
        except ValueError:
            logger.warning(
                "detail: resposta 200 inválida (JSON) para ticket_id=%s em endpoint=%s",
                ticket_id,
                endpoint,
            )
            return None, "invalid_json", {
                "endpoint": endpoint,
                "status_code": status,
                "body": body_snippet,
            }

        if isinstance(data, list):
            ticket = data[0] if data else None
        else:
            ticket = data

        if not ticket:
            logger.info(
                "detail: endpoint=%s ticket_id=%s retornou 200 mas corpo vazio",
                endpoint,
                ticket_id,
            )
            return None, "empty_200", {
                "endpoint": endpoint,
                "status_code": status,
                "body": body_snippet,
            }

        return ticket, "ok", {"endpoint": endpoint, "status_code": status, "body": ""}

    if status == 404:
        logger.info(
            "detail: ticket_id=%s não encontrado (404) em endpoint=%s",
            ticket_id,
            endpoint,
        )
        return None, "not_found", {
            "endpoint": endpoint,
            "status_code": status,
            "body": body_snippet,
        }

    reason = f"http_error_{status}"
    logger.warning(
        "detail: chamada endpoint=%s para ticket_id=%s retornou %s. Corpo (trecho): %s",
        endpoint,
        ticket_id,
        status,
        body_snippet,
    )
    return None, reason, {
        "endpoint": endpoint,
        "status_code": status,
        "body": body_snippet,
    }


def fetch_ticket_with_fallback(
    ticket_id: int, ref_date: Optional[datetime]
) -> Tuple[Optional[Dict[str, Any]], str, Dict[str, Any]]:
    """
    Busca detalhe do ticket usando /tickets e /tickets/past.

    - Se ref_date for mais antiga que 90 dias, tenta /tickets/past primeiro.
    - Só tenta o segundo endpoint se o primeiro retornar "not_found".
    """
    prefer_past = _should_use_past_first(ref_date)
    first, second = ("tickets/past", "tickets") if prefer_past else ("tickets", "tickets/past")

    logger.info(
        "detail: fetch_ticket_with_fallback ticket_id=%s ref_date=%s prefer_past=%s first=%s second=%s",
        ticket_id,
        ref_date,
        prefer_past,
        first,
        second,
    )

    # 1ª tentativa
    ticket, reason, meta = _fetch_ticket_once(ticket_id, first, ref_date)
    if ticket is not None or reason != "not_found":
        # Achou ou falhou de um jeito que não faz sentido tentar o outro endpoint
        return ticket, reason, meta

    # Só cai aqui se reason == "not_found"
    ticket2, reason2, meta2 = _fetch_ticket_once(ticket_id, second, ref_date)
    if ticket2 is not None:
        return ticket2, "ok", meta2

    if reason2 != "not_found":
        return None, reason2, meta2

    return None, "not_found_both", meta2


def fetch_pending_from_audit(conn, limit: int) -> List[Tuple[int, Optional[datetime]]]:
    """
    Busca na audit_recent_missing os tickets da tabela tickets_resolvidos
    que ainda precisam ter o detalhe sincronizado.
    """
    sql = """
        select ticket_id, ref_date
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = 'tickets_resolvidos'
        order by ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return rows


def build_detail_row(ticket: Dict[str, Any]) -> Tuple[
    int,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """Extrai os campos relevantes do JSON do ticket."""
    ticket_id = int(ticket.get("id") or ticket.get("ticketId"))

    status = ticket.get("status")
    last_update = ticket.get("lastUpdate")
    last_resolved_at = ticket.get("resolvedIn")
    last_closed_at = ticket.get("closedIn")
    last_cancelled_at = ticket.get("canceledIn") or ticket.get("cancelledIn")

    return (
        ticket_id,
        status,
        last_update,
        last_resolved_at,
        last_closed_at,
        last_cancelled_at,
    )


def upsert_details(conn, tickets: List[Dict[str, Any]]) -> None:
    """Faz upsert dos detalhes na visualizacao_resolvidos.tickets_resolvidos."""
    if not tickets:
        return

    rows = [build_detail_row(t) for t in tickets]

    sql = """
        insert into visualizacao_resolvidos.tickets_resolvidos (
            ticket_id,
            status,
            last_update,
            last_resolved_at,
            last_closed_at,
            last_cancelled_at
        ) values %s
        on conflict (ticket_id) do update set
            status = excluded.status,
            last_update = excluded.last_update,
            last_resolved_at = excluded.last_resolved_at,
            last_closed_at = excluded.last_closed_at,
            last_cancelled_at = excluded.last_cancelled_at
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)

    logger.info(
        "detail: detail: %s tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.",
        len(rows),
    )


def register_failure(conn, ticket_id: int, reason: str, meta: Dict[str, Any]) -> None:
    """
    Tenta registrar a falha em visualizacao_resolvidos.audit_ticket_watch.

    Se o insert der erro (diferença de estrutura, etc.), só loga um warning
    para não derrubar o job.
    """
    endpoint = meta.get("endpoint")
    status_code = meta.get("status_code")
    body = meta.get("body") or ""
    snippet = body[:900]

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into visualizacao_resolvidos.audit_ticket_watch
                    (ticket_id, endpoint, status_code, reason, body)
                values (%s, %s, %s, %s, %s)
                """,
                (ticket_id, endpoint, status_code, reason, snippet),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "detail: falha ao registrar em audit_ticket_watch (ticket_id=%s, reason=%s): %s",
            ticket_id,
            reason,
            exc,
        )


def main() -> None:
    if not DSN:
        raise RuntimeError("Variável de ambiente NEON_DSN não configurada.")
    if not API_TOKEN:
        raise RuntimeError("Variável de ambiente MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN) não configurada.")

    limit = PAGE_LIMIT

    with psycopg2.connect(DSN) as conn:
        pending = fetch_pending_from_audit(conn, limit)
        total = len(pending)

        if total == 0:
            logger.info("detail: detail: nenhum ticket pendente para atualização de detalhes.")
            return

        logger.info(
            "detail: detail: %s tickets pendentes para atualização de detalhes (limite=%s).",
            total,
            limit,
        )

        preview_parts = []
        for i, (tid, ref_date) in enumerate(pending[:5], start=1):
            preview_parts.append(
                f"{i}) id={tid}, ref_date={ref_date}, prefer_past={_should_use_past_first(ref_date)}"
            )
        if preview_parts:
            logger.info("detail: detail: primeiros pendentes (até 5): %s", " | ".join(preview_parts))

        detalhes_ok: List[Dict[str, Any]] = []
        fail_counts: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for idx, (ticket_id, ref_date) in enumerate(pending, start=1):
            logger.info(
                "detail: detail: processando ticket %s/%s (ticket_id=%s, ref_date=%s)",
                idx,
                total,
                ticket_id,
                ref_date,
            )

            ticket, reason, meta = fetch_ticket_with_fallback(ticket_id, ref_date)

            if ticket is not None:
                detalhes_ok.append(ticket)
            else:
                fail_counts[reason] = fail_counts.get(reason, 0) + 1
                fail_samples.setdefault(reason, ticket_id)
                register_failure(conn, ticket_id, reason, meta)

        ok = len(detalhes_ok)
        fails = sum(fail_counts.values())

        logger.info(
            "detail: detail: processados neste ciclo: ok=%s, falhas=%s.",
            ok,
            fails,
        )

        if fail_counts:
            logger.info("detail: detail: razões de falha neste ciclo:")
            for reason, count in fail_counts.items():
                sample = fail_samples.get(reason)
                logger.info(
                    "detail: detail:   - %s: %s tickets (exemplo ticket_id=%s)",
                    reason,
                    count,
                    sample,
                )

        if ok:
            upsert_details(conn, detalhes_ok)
        else:
            logger.info(
                "detail: detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch."
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logger.exception("detail: erro inesperado no processamento de detalhes: %s", exc)
        raise
