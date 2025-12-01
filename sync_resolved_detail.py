import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values


# -------------------------------------------------------------------
# Config / ambiente
# -------------------------------------------------------------------

API_BASE = "https://api.movidesk.com/public/v1"

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("PG_DSN")

# Lote de tickets a processar por execução
BATCH = int(os.getenv("DETAIL_BATCH") or os.getenv("PAGES_UPSERT", "200"))

# Intervalo entre tickets (para não espancar a API)
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

# Tentativas máximas para tratar HTTP 429 respeitando Retry-After
MAX_429_RETRIES = int(os.getenv("MAX_429_RETRIES", "3"))

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


# -------------------------------------------------------------------
# Helpers de API
# -------------------------------------------------------------------

def _parse_retry_after(headers: Dict[str, str]) -> Optional[int]:
    """Lê o header Retry-After (segundos)."""
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if not retry_after:
        return None
    try:
        return int(retry_after)
    except ValueError:
        return None


def md_get(
    path_or_full: str,
    params: Optional[Dict[str, Any]] = None,
    ok_404: bool = False,
) -> Any:
    """
    GET na API do Movidesk com tratamento de erros transitórios,
    respeitando o header Retry-After para 429 (Too Many Requests).

    Se ok_404=True, retorna None em caso de 404 ao invés de levantar erro.
    """
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    base_params = dict(params or {})
    base_params["token"] = MOVIDESK_TOKEN

    last_exc: Optional[requests.HTTPError] = None

    for attempt in range(1, MAX_429_RETRIES + 1):
        resp = SESSION.get(url, params=base_params, timeout=60)

        # Sucesso
        if resp.status_code == 200:
            return resp.json() or {}

        # 404 "controlado"
        if ok_404 and resp.status_code == 404:
            return None

        # 429 => respeita Retry-After
        if resp.status_code == 429:
            wait = _parse_retry_after(resp.headers)
            if wait is None:
                # Manual: 60 / 120 / 300; fallback conservador crescente
                wait = 60 * attempt
            LOG.warning(
                "HTTP 429 em %s (tentativa %d/%d, Retry-After=%s). Aguardando %s segundos...",
                url,
                attempt,
                MAX_429_RETRIES,
                resp.headers.get("Retry-After"),
                wait,
            )
            time.sleep(wait)
            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:  # guarda exceção mais recente
                last_exc = exc
            continue

        # Erros 5xx => tenta de novo com backoff simples
        if resp.status_code in (500, 502, 503, 504):
            backoff = 2 * attempt
            LOG.warning(
                "HTTP %s em %s (tentativa %d/%d). Aguardando %s segundos...",
                resp.status_code,
                url,
                attempt,
                MAX_429_RETRIES,
                backoff,
            )
            time.sleep(backoff)
            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                last_exc = exc
            continue

        # Demais códigos: 400, 401 etc -> erro "definitivo"
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            last_exc = exc
        break

    # Se chegou aqui é porque não retornou 200/404 controlado
    if last_exc is not None:
        raise last_exc
    resp.raise_for_status()


# -------------------------------------------------------------------
# Banco de dados
# -------------------------------------------------------------------

def get_pending_tickets(conn, limit: int) -> List[Tuple[int, Optional[datetime]]]:
    """
    Busca os tickets pendentes em visualizacao_resolvidos.audit_recent_missing
    para a tabela 'tickets_resolvidos', junto com uma data de referência
    (ref_date) para decidir se preferimos /tickets ou /tickets/past.

    ref_date = COALESCE(
        last_resolved_at,
        last_cancelled_at,
        last_closed_at,
        last_update
    ) da tabela de destino, quando existir.
    """
    sql = """
        SELECT DISTINCT ON (m.ticket_id)
               m.ticket_id,
               COALESCE(
                   t.last_resolved_at,
                   t.last_cancelled_at,
                   t.last_closed_at,
                   t.last_update
               ) AS ref_date
          FROM visualizacao_resolvidos.audit_recent_missing AS m
          LEFT JOIN visualizacao_resolvidos.tickets_resolvidos AS t
            ON t.ticket_id = m.ticket_id
         WHERE m.table_name = 'tickets_resolvidos'
         ORDER BY m.ticket_id DESC, m.run_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    result: List[Tuple[int, Optional[datetime]]] = []
    for ticket_id, ref_date in rows:
        result.append((int(ticket_id), ref_date))
    return result


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra/atualiza falhas em audit_ticket_watch, sem estourar chave primária.
    PK conhecida: (ticket_id).

    A coluna de motivo não é usada aqui; o motivo fica apenas no log.
    """
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
    except Exception as e:  # pragma: no cover
        LOG.error("detail: erro ao registrar falha em audit_ticket_watch (%s)", e)


# -------------------------------------------------------------------
# Transformação de dados do ticket
# -------------------------------------------------------------------

def parse_movidesk_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Converte string de data/hora da Movidesk em datetime do Python
    usando apenas a stdlib (sem dateutil).

    Aceita formatos ISO, com ou sem 'Z' no final.
    """
    if not value:
        return None
    try:
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def build_detail_row(ticket: Dict[str, Any]) -> Tuple[Any, ...]:
    """
    Monta a tupla na ordem exata das colunas de
    visualizacao_resolvidos.tickets_resolvidos usadas hoje.
    """
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

    # Regras de preenchimento para as colunas de resolved/closed/cancelled
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

    # timestamp de quando esse registro foi inserido pela primeira vez
    adicionado_em_tabela = datetime.now(timezone.utc)

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
        adicionado_em_tabela,
    )


# -------------------------------------------------------------------
# Upsert no banco
# -------------------------------------------------------------------

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
      adicional_nome,
      adicionado_em_tabela
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
      adicional_nome       = EXCLUDED.adicional_nome,
      -- mantém o timestamp original, só preenchendo quando ainda está NULL
      adicionado_em_tabela = COALESCE(
          visualizacao_resolvidos.tickets_resolvidos.adicionado_em_tabela,
          EXCLUDED.adicionado_em_tabela
      )
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()


# -------------------------------------------------------------------
# Busca com múltiplas estratégias (/tickets e /tickets/past)
# -------------------------------------------------------------------

SELECT_FIELDS = (
    "id,status,lastUpdate,origin,category,urgency,"
    "serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,"
    "owner,organization,clients,subject,actions,customFields,createdBy"
)

EXPAND_FULL = "clients,createdBy,owner,actions,customFields"


def _should_use_past_first(ref_date: Optional[datetime]) -> bool:
    """
    Decide se devemos tentar /tickets/past primeiro.

    Regra: se tivermos uma ref_date e ela for mais antiga que 90 dias atrás,
    então preferimos buscar em /tickets/past.
    """
    if ref_date is None:
        return False

    # Normaliza para UTC e garante timezone-aware para evitar
    # "can't compare offset-naive and offset-aware datetimes".
    if ref_date.tzinfo is None:
        ref_date = ref_date.replace(tzinfo=timezone.utc)
    else:
        ref_date = ref_date.astimezone(timezone.utc)

    now_utc = datetime.now(timezone.utc)
    return ref_date < now_utc - timedelta(days=90)


def _fetch_from_list_endpoint(endpoint: str, ticket_id: int) -> Optional[Dict[str, Any]]:
    """
    Consulta um endpoint de lista ('tickets' ou 'tickets/past') usando
    $filter=id eq {id}. Retorna o primeiro item ou None.
    """
    LOG.info(
        "detail: consultando endpoint=%s para ticket_id=%s",
        endpoint,
        ticket_id,
    )
    data = md_get(
        endpoint,
        params={
            "$filter": f"id eq {ticket_id}",
            "$select": SELECT_FIELDS,
            "$expand": EXPAND_FULL,
        },
        ok_404=True,
    )
    if not data:
        return None
    if isinstance(data, list):
        return data[0]
    return data


def fetch_ticket_with_fallback(
    ticket_id: int,
    ref_date: Optional[datetime],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Tenta buscar detalhes do ticket usando duas estratégias, escolhendo a ordem
    com base em ref_date (regra de 90 dias):

      - /tickets
      - /tickets/past

    Se prefer_past=True, tenta primeiro /tickets/past, depois /tickets.
    Caso contrário, faz o inverso.

    Retorna (ticket_dict, reason). Se reason for None, a busca foi bem-sucedida.
    """
    prefer_past = _should_use_past_first(ref_date)
    first_endpoint, second_endpoint = (
        ("tickets/past", "tickets") if prefer_past else ("tickets", "tickets/past")
    )

    LOG.info(
        "detail: fetch_ticket_with_fallback ticket_id=%s ref_date=%s "
        "prefer_past=%s first=%s second=%s",
        ticket_id,
        ref_date,
        prefer_past,
        first_endpoint,
        second_endpoint,
    )

    last_reason: Optional[str] = None

    # 1) endpoint preferencial
    try:
        ticket = _fetch_from_list_endpoint(first_endpoint, ticket_id)
        if ticket is not None:
            LOG.info(
                "detail: ticket_id=%s encontrado em endpoint=%s",
                ticket_id,
                first_endpoint,
            )
            return ticket, None
        last_reason = "not_found_first"
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        last_reason = f"http_error_{status or 'unknown'}"
    except Exception:
        last_reason = "exception_api_first"

    # 2) endpoint alternativo
    try:
        ticket = _fetch_from_list_endpoint(second_endpoint, ticket_id)
        if ticket is not None:
            LOG.info(
                "detail: ticket_id=%s encontrado em endpoint=%s",
                ticket_id,
                second_endpoint,
            )
            return ticket, None
        last_reason = last_reason or "not_found_second"
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        last_reason = f"http_error_{status or 'unknown'}"
    except Exception:
        last_reason = "exception_api_second"

    return None, last_reason or "unknown"


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_tickets(conn, BATCH)
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

        # Loga os primeiros pendentes para debug
        preview_items: List[str] = []
        for idx, (tid, ref_date) in enumerate(pending[:5], start=1):
            prefer_past = _should_use_past_first(ref_date)
            preview_items.append(
                f"{idx}) id={tid}, ref_date={ref_date}, prefer_past={prefer_past}"
            )
        if preview_items:
            LOG.info(
                "detail: primeiros pendentes (até 5): %s",
                " | ".join(preview_items),
            )

        detalhes: List[Tuple[Any, ...]] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = []

        for idx, (tid, ref_date) in enumerate(pending, start=1):
            LOG.info(
                "detail: processando ticket %d/%d (ticket_id=%s, ref_date=%s)",
                idx,
                total_pendentes,
                tid,
                ref_date,
            )

            ticket, reason = fetch_ticket_with_fallback(tid, ref_date)

            if ticket is None:
                # Falha em todas as estratégias
                reason = reason or "unknown"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                # pequena pausa entre erros para não acumular "failed requests"
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
            time.sleep(THROTTLE)

        total_ok = len(detalhes)
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
            # Nessa abordagem, a trigger no banco é quem cuida do audit_recent_missing.
            return

        # Upsert; trigger no DB cuida de remover de audit_recent_missing
        upsert_details(conn, detalhes)
        LOG.info(
            "detail: %d tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.",
            len(detalhes),
        )


if __name__ == "__main__":
    main()
