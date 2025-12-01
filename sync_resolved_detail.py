#!/usr/bin/env python
"""
Sincroniza detalhes de tickets Movidesk para a tabela
visualizacao_resolvidos.tickets_resolvidos.

- Busca IDs pendentes em visualizacao_resolvidos.audit_recent_missing
  (apenas para table_name = 'tickets_resolvidos').
- Para cada ticket, tenta consultar o endpoint /tickets; se não encontrar
  ou der erro 4xx/5xx, tenta /tickets/past.
- Monta o "detalhe" (status + datas principais) e faz upsert na tabela
  de destino.
- NÃO remove da audit_recent_missing: há trigger no banco que cuida disso
  quando a linha é inserida/atualizada em tickets_resolvidos.
"""

from __future__ import annotations

import os
import sys
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values


# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------

LOG_NAME = "detail"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(LOG_NAME)

DSN = os.environ.get("NEON_DSN")
if not DSN:
    logger.error("%s: variável de ambiente NEON_DSN não configurada.", LOG_NAME)
    sys.exit(1)

API_TOKEN = (
    os.environ.get("MOVIDESK_TOKEN")
    or os.environ.get("MOVIDESK_API_TOKEN")
)
if not API_TOKEN:
    logger.error(
        "%s: variável de ambiente MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN não configurada.",
        LOG_NAME,
    )
    sys.exit(1)

BASE_URL = "https://api.movidesk.com/public/v1"
DEFAULT_LIMIT = 50

try:
    LIMIT = int(os.environ.get("PAGES_UPSERT", str(DEFAULT_LIMIT)))
except ValueError:
    LIMIT = DEFAULT_LIMIT


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PendingTicket:
    ticket_id: int


@dataclass
class TicketDetail:
    ticket_id: int
    status: Optional[str]
    last_update: Optional[datetime]
    last_resolved_at: Optional[datetime]
    last_closed_at: Optional[datetime]
    last_cancelled_at: Optional[datetime]
    adicionado_em_tabela: datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Tenta converter string ISO 8601 em datetime aware (UTC)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            # assume UTC se vier sem timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _http_get_ticket(
    endpoint: str, ticket_id: int
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    """
    Faz GET no endpoint indicado.

    Retorna (json, reason, meta):
      - json: dict com o ticket em caso de sucesso, senão None
      - reason: string explicando o erro, ou None se sucesso
      - meta: infos auxiliares (endpoint, status_code, etc.)
    """
    url = f"{BASE_URL}/{endpoint}/{ticket_id}"
    params = {
        "token": API_TOKEN,
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
    except requests.RequestException as exc:
        return None, "http_exception", {
            "endpoint": endpoint,
            "exception": str(exc),
        }

    meta: Dict[str, Any] = {
        "endpoint": endpoint,
        "status_code": resp.status_code,
    }

    if resp.status_code == 404:
        return None, "http_error_404", meta
    if resp.status_code >= 400:
        meta["body"] = resp.text[:500]
        return None, "http_error_400", meta

    try:
        data = resp.json()
    except ValueError as exc:
        meta["body"] = resp.text[:500]
        meta["exception"] = str(exc)
        return None, "json_decode_error", meta

    return data, None, meta


def build_ticket_detail(ticket_id: int, raw: Dict[str, Any]) -> TicketDetail:
    """
    Extrai os campos relevantes do JSON do Movidesk.

    Usamos vários possíveis nomes de campos para ser mais resiliente
    a mudanças na API.
    """
    status = raw.get("status")

    # nomes possíveis para datas na API do Movidesk
    last_update = (
        raw.get("lastUpdate")
        or raw.get("lastUpdateDate")
        or raw.get("lastUpdatedDate")
        or raw.get("lastUpdateDateTime")
    )
    last_resolved = raw.get("resolvedDate") or raw.get("resolvedAt")
    last_closed = raw.get("closedDate") or raw.get("closedAt")
    last_cancelled = (
        raw.get("canceledDate")
        or raw.get("cancelledDate")
        or raw.get("canceledAt")
        or raw.get("cancelledAt")
    )

    now_utc = datetime.now(timezone.utc)

    return TicketDetail(
        ticket_id=ticket_id,
        status=status,
        last_update=_parse_datetime(last_update),
        last_resolved_at=_parse_datetime(last_resolved),
        last_closed_at=_parse_datetime(last_closed),
        last_cancelled_at=_parse_datetime(last_cancelled),
        adicionado_em_tabela=now_utc,
    )


def fetch_ticket_with_fallback(
    ticket_id: int,
) -> Tuple[Optional[TicketDetail], str, Dict[str, Any]]:
    """
    Tenta buscar o ticket primeiro em /tickets, depois em /tickets/past.

    Retorna (TicketDetail|None, reason, meta). Se o ticket for encontrado
    em algum dos endpoints, reason será "ok".
    """
    last_reason: str = "unknown_error"
    last_meta: Dict[str, Any] = {}

    for endpoint in ("tickets", "tickets/past"):
        logger.info(
            "%s: consultando endpoint=%s para ticket_id=%s",
            LOG_NAME,
            endpoint,
            ticket_id,
        )
        data, reason, meta = _http_get_ticket(endpoint, ticket_id)
        if data is not None:
            logger.info(
                "%s: ticket_id=%s encontrado em endpoint=%s",
                LOG_NAME,
                ticket_id,
                endpoint,
            )
            detail = build_ticket_detail(ticket_id, data)
            return detail, "ok", {"endpoint": endpoint}

        # guarda último erro e tenta o próximo endpoint
        last_reason = reason or "unknown_error"
        last_meta = meta

    # se chegou aqui, falhou em todos os endpoints
    return None, last_reason, last_meta


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def fetch_pending_from_audit(conn, limit: int) -> List[PendingTicket]:
    """
    Busca IDs pendentes na audit_recent_missing.

    Importante: não usamos nenhuma coluna além de table_name e ticket_id
    para evitar problemas com alterações de schema.
    """
    sql = """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = 'tickets_resolvidos'
        order by ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    pending = [PendingTicket(ticket_id=row[0]) for row in rows]
    return pending


def upsert_details(conn, detalhes: Sequence[TicketDetail]) -> None:
    """
    Faz upsert dos detalhes na tabela de destino.

    NÃO remove nada da audit_recent_missing: isso é responsabilidade
    de trigger no banco.
    """
    if not detalhes:
        return

    rows = [
        (
            d.ticket_id,
            d.status,
            d.last_update,
            d.last_resolved_at,
            d.last_closed_at,
            d.last_cancelled_at,
            d.adicionado_em_tabela,
        )
        for d in detalhes
    ]

    sql = """
        insert into visualizacao_resolvidos.tickets_resolvidos as t (
            ticket_id,
            status,
            last_update,
            last_resolved_at,
            last_closed_at,
            last_cancelled_at,
            adicionado_em_tabela
        )
        values %s
        on conflict (ticket_id) do update set
            status = excluded.status,
            last_update = excluded.last_update,
            last_resolved_at = excluded.last_resolved_at,
            last_closed_at = excluded.last_closed_at,
            last_cancelled_at = excluded.last_cancelled_at,
            adicionado_em_tabela = excluded.adicionado_em_tabela
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)


def register_failure(ticket_id: int, reason: str, meta: Dict[str, Any]) -> None:
    """
    Registra falhas apenas no log.

    Se quiser persistir em tabela (visualizacao_resolvidos.audit_ticket_watch),
    dá pra adicionar o insert aqui depois.
    """
    logger.info(
        "%s: falha ao buscar ticket_id=%s reason=%s meta=%s",
        LOG_NAME,
        ticket_id,
        reason,
        meta,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logger.info(
        "%s: iniciando sincronização de detalhes de tickets (limite=%s).",
        LOG_NAME,
        LIMIT,
    )

    try:
        with psycopg2.connect(DSN) as conn:
            pending = fetch_pending_from_audit(conn, LIMIT)
            total = len(pending)
            if not total:
                logger.info("%s: nenhum ticket pendente em audit_recent_missing.", LOG_NAME)
                return

            preview = " | ".join(
                f"{i+1}) id={p.ticket_id}"
                for i, p in enumerate(pending[:5])
            )
            logger.info(
                "%s: %s tickets pendentes para atualização de detalhes (limite=%s).",
                LOG_NAME,
                total,
                LIMIT,
            )
            logger.info("%s: primeiros pendentes (até 5): %s", LOG_NAME, preview)

            detalhes_ok: List[TicketDetail] = []
            fail_counts: Dict[str, int] = {}
            fail_samples: Dict[str, int] = {}

            for idx, p in enumerate(pending, start=1):
                ticket_id = p.ticket_id
                logger.info(
                    "%s: processando ticket %s/%s (ticket_id=%s)",
                    LOG_NAME,
                    idx,
                    total,
                    ticket_id,
                )

                detail, reason, meta = fetch_ticket_with_fallback(ticket_id)

                if detail is not None:
                    detalhes_ok.append(detail)
                else:
                    # registra falha
                    register_failure(ticket_id, reason, meta)
                    fail_counts[reason] = fail_counts.get(reason, 0) + 1
                    fail_samples.setdefault(reason, ticket_id)

            ok_count = len(detalhes_ok)
            fail_total = sum(fail_counts.values())

            if detalhes_ok:
                upsert_details(conn, detalhes_ok)

            logger.info(
                "%s: processados neste ciclo: ok=%s, falhas=%s.",
                LOG_NAME,
                ok_count,
                fail_total,
            )

            if fail_counts:
                logger.info("%s: razões de falha neste ciclo:", LOG_NAME)
                for reason, count in fail_counts.items():
                    sample = fail_samples.get(reason)
                    logger.info(
                        "%s:   - %s: %s tickets (exemplo ticket_id=%s)",
                        LOG_NAME,
                        reason,
                        count,
                        sample,
                    )

            if ok_count == 0:
                logger.info(
                    "%s: nenhum ticket com detalhe válido; apenas falhas registradas em log.",
                    LOG_NAME,
                )

    except Exception as exc:
        logger.error(
            "%s: erro inesperado no processamento de detalhes: %s",
            LOG_NAME,
            exc,
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
