import os
import time
import logging
from typing import Any, Dict, List, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import errors
from dateutil import parser  # já estava sendo usado na versão com actions


# --------------------------------------------------------------------
# Configuração básica
# --------------------------------------------------------------------

API_BASE = "https://api.movidesk.com/public/v1"

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN") or os.getenv("PG_DSN")

BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

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


# --------------------------------------------------------------------
# Função de chamada da API Movidesk
# --------------------------------------------------------------------

def md_get(path_or_full: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False):
    """
    GET simples na API Movidesk com tratamento de 429/5xx e 404 opcional.
    """
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = MOVIDESK_TOKEN

    r = SESSION.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or {}

    if ok_404 and r.status_code == 404:
        return None

    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = SESSION.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or {}
        if ok_404 and r2.status_code == 404:
            return None
        r2.raise_for_status()

    r.raise_for_status()


# --------------------------------------------------------------------
# Consulta de pendências (com fallback)
# --------------------------------------------------------------------

def get_pending_ids_from_missing(conn, limit: int) -> List[int]:
    """
    Busca tickets pendentes em visualizacao_resolvidos.audit_recent_missing
    usando audit_recent_run para ordenar pelos mais recentes.
    """
    sql = """
        SELECT DISTINCT m.ticket_id
          FROM visualizacao_resolvidos.audit_recent_missing m
          JOIN visualizacao_resolvidos.audit_recent_run   r ON r.id = m.run_id
         WHERE m.table_name = 'tickets_resolvidos'
         ORDER BY r.run_at DESC, m.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]


def get_pending_ids_from_tickets(conn, limit: int) -> List[int]:
    """
    Fallback: busca diretamente em visualizacao_resolvidos.tickets_resolvidos
    tickets Resolvido/Fechado/Cancelado com last_resolved_at ou last_closed_at nulos.
    """
    sql = """
        SELECT t.ticket_id
          FROM visualizacao_resolvidos.tickets_resolvidos t
         WHERE (t.last_resolved_at IS NULL OR t.last_closed_at IS NULL)
           AND t.status IN ('Resolvido', 'Fechado', 'Cancelado')
         ORDER BY t.ticket_id DESC
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]


def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Primeiro tenta audit_recent_missing; se der erro, cai para tickets_resolvidos.
    """
    try:
        return get_pending_ids_from_missing(conn, limit)
    except errors.UndefinedTable:
        conn.rollback()
        LOG.info(
            "detail: tabela audit_recent_missing não existe neste banco. "
            "Caindo para busca direta em tickets_resolvidos."
        )
    except Exception as e:
        conn.rollback()
        LOG.error(
            "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s). "
            "Caindo para tickets_resolvidos.",
            e,
        )

    try:
        return get_pending_ids_from_tickets(conn, limit)
    except errors.UndefinedTable:
        conn.rollback()
        LOG.info(
            "detail: tabela visualizacao_resolvidos.tickets_resolvidos não existe neste banco. "
            "Nada para processar."
        )
        return []
    except Exception as e:
        conn.rollback()
        LOG.error(
            "detail: erro ao consultar visualizacao_resolvidos.tickets_resolvidos (%s). "
            "Nada para processar.",
            e,
        )
        return []


# --------------------------------------------------------------------
# Auditoria de falhas
# --------------------------------------------------------------------

def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra (ou atualiza) falha em visualizacao_resolvidos.audit_ticket_watch
    sem gerar erro de chave duplicada.
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
                    table_name  = EXCLUDED.table_name,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (ticket_id,),
            )
        conn.commit()
    except Exception as e:
        # Não deixamos a sync quebrar por causa da auditoria
        LOG.error(
            "detail: erro ao registrar falha em audit_ticket_watch (%s)", e
        )


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    """
    Remove do audit_recent_missing apenas os tickets processados com sucesso.
    """
    if not ids:
        return

    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
         WHERE table_name = 'tickets_resolvidos'
           AND ticket_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()


# --------------------------------------------------------------------
# Montagem da linha de detalhe (corrigindo as datas)
# --------------------------------------------------------------------

def build_detail_row(ticket: Dict[str, Any]):
    """
    Usa o histórico de actions (status + createdDate) para montar:
      - last_resolved_at_db
      - last_closed_at_db
      - last_cancelled_at (a partir do status Cancelado/Canceled)
    Aplicando as regras de negócio já definidas:
      - Resolvido  -> preenche last_resolved_at, last_closed_at = NULL
      - Fechado    -> preserva last_resolved_at da resolução e preenche last_closed_at
      - Cancelado  -> last_resolved_at = NULL, last_closed_at = NULL
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

        try:
            dt = parser.isoparse(created)
        except Exception:
            # Se vier alguma data malformada, ignora essa action e segue
            continue

        if status in ("Resolvido", "Resolved"):
            last_resolved_at = dt

        if status in ("Fechado", "Closed"):
            last_closed_at = dt

        if status in ("Cancelado", "Canceled"):
            canceled_at = dt

    final_status = (ticket.get("status") or "").strip()

    # Aplica as regras de preenchimento considerando o status final
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
        # Outros status (caso apareçam aqui por algum motivo)
        last_resolved_at_db = last_resolved_at
        last_closed_at_db = last_closed_at

    owner = ticket.get("owner") or {}
    org = ticket.get("organization") or {}
    clients = ticket.get("clients") or []
    client = clients[0] if clients else {}

    # Ordem dos campos 100% alinhada com o INSERT em tickets_resolvidos
    return (
        int(ticket["id"]),
        final_status or None,
        last_resolved_at_db,
        last_closed_at_db,
        canceled_at,  # last_cancelled_at
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
    )


# --------------------------------------------------------------------
# Upsert na tabela visualizacao_resolvidos.tickets_resolvidos
# --------------------------------------------------------------------

def upsert_details(conn, rows: List[tuple]) -> None:
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
      adicional_nome
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
      adicional_nome       = EXCLUDED.adicional_nome
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def main():
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            LOG.info("detail: nenhum ticket pendente em audit_recent_missing/tickets_resolvidos.")
            return

        LOG.info(
            "detail: %d tickets pendentes para atualização de detalhes (limite=%d).",
            total_pendentes,
            BATCH,
        )

        detalhes: List[tuple] = []
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
                status = getattr(e.response, "status_code", None)
                reason = f"http_error_{status or 'unknown'}"
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
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        LOG.info(
            "detail: %d tickets upsertados em visualizacao_resolvidos.tickets_resolvidos "
            "e removidos de visualizacao_resolvidos.audit_recent_missing.",
            len(ok_ids),
        )


if __name__ == "__main__":
    main()
