import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values

# -------------------------------------------------------
# Configuração básica
# -------------------------------------------------------

API_BASE = os.getenv("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN:
    raise RuntimeError("Defina MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN")
if not DSN:
    raise RuntimeError("Defina NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] detail: %(message)s",
)
logger = logging.getLogger("detail")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})


# -------------------------------------------------------
# Função de acesso à API do Movidesk
# -------------------------------------------------------

def md_get(path_or_full, params=None, ok_404=False):
    """
    Faz GET na API pública do Movidesk.
    Se ok_404=True, devolve None em caso de 404.
    """
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN

    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        try:
            return r.json() or {}
        except ValueError:
            return {}

    if ok_404 and r.status_code == 404:
        return None

    # retry simples pra 429/5xx
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            try:
                return r2.json() or {}
            except ValueError:
                return {}
        if ok_404 and r2.status_code == 404:
            return None

    r.raise_for_status()


# -------------------------------------------------------
# SQL
# -------------------------------------------------------

# Caminho principal: usar audit_recent_missing
SQL_GET_PENDING_FROM_AUDIT = """
SELECT ticket_id
  FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
 GROUP BY ticket_id
 ORDER BY max(run_id) DESC, ticket_id DESC
 LIMIT %s
"""

# Fallback: buscar em tickets_resolvidos quando o audit não puder ser usado
SQL_GET_PENDING_FROM_TICKETS = """
SELECT ticket_id
  FROM visualizacao_resolvidos.tickets_resolvidos
 WHERE status IS NULL
    OR last_update IS NULL
 ORDER BY ticket_id DESC
 LIMIT %s
"""

SQL_DELETE_FROM_MISSING = """
DELETE FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
   AND ticket_id = ANY(%s)
"""

SQL_REGISTER_FAILURE = """
INSERT INTO visualizacao_resolvidos.audit_ticket_watch (ticket_id, table_name, last_seen_at)
VALUES (%s, 'tickets_resolvidos', now())
ON CONFLICT (ticket_id) DO UPDATE
   SET table_name = EXCLUDED.table_name,
       last_seen_at = EXCLUDED.last_seen_at
"""

SQL_UPSERT_DETAILS = """
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


# -------------------------------------------------------
# Helpers de banco
# -------------------------------------------------------

def register_ticket_failure(conn, ticket_id, reason):
    """
    Registra (ou atualiza) falha em audit_ticket_watch.
    Não deixa a falha derrubar o processo.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_REGISTER_FAILURE, (ticket_id,))
        conn.commit()
    except Exception as e:
        logger.error(
            "detail: erro ao registrar falha em audit_ticket_watch (%s)", e
        )
        conn.rollback()


def delete_from_missing(conn, ids):
    if not ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_DELETE_FROM_MISSING, (ids,))
        conn.commit()
    except Exception as e:
        logger.error(
            "detail: erro ao remover tickets de audit_recent_missing (%s)", e
        )
        conn.rollback()


def get_pending_ids(conn, limit):
    """
    Tenta pegar IDs pendentes primeiro de audit_recent_missing.
    Se der erro, cai pro fallback em tickets_resolvidos.
    Retorna (lista_ids, origem) onde origem é 'audit', 'tickets' ou 'none'.
    """
    if not limit or limit <= 0:
        limit = BATCH

    # 1) tenta audit_recent_missing
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_GET_PENDING_FROM_AUDIT, (limit,))
            rows = cur.fetchall()
        ids = [r[0] for r in rows]
        if ids:
            logger.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
                len(ids),
                limit,
            )
            return ids, "audit"
    except Exception as e:
        logger.error(
            "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s). Caindo para tickets_resolvidos.",
            e,
        )

    # 2) fallback: tickets_resolvidos
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_GET_PENDING_FROM_TICKETS, (limit,))
            rows = cur.fetchall()
        ids = [r[0] for r in rows]
        if ids:
            logger.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
                len(ids),
                limit,
            )
        else:
            logger.info(
                "detail: nenhum ticket pendente para processar em visualizacao_resolvidos.tickets_resolvidos."
            )
        return ids, "tickets"
    except Exception as e:
        logger.error(
            "detail: erro ao consultar visualizacao_resolvidos.tickets_resolvidos (%s).",
            e,
        )
        return [], "none"


# -------------------------------------------------------
# Construção da linha pra tickets_resolvidos
# -------------------------------------------------------

def build_detail_row(ticket):
    owner = ticket.get("owner") or {}
    org = ticket.get("organization") or {}
    clients = ticket.get("clients") or []
    client = clients[0] if clients else {}

    return (
        int(ticket["id"]),
        ticket.get("status"),
        ticket.get("lastResolvedDate"),
        ticket.get("lastClosedDate"),
        ticket.get("lastCancelledDate"),
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


def upsert_details(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, SQL_UPSERT_DETAILS, rows, page_size=200)
    conn.commit()


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def main():
    with psycopg2.connect(DSN) as conn:
        pending, source = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)

        if not pending:
            logger.info("detail: nenhum ticket pendente para processar.")
            return

        detalhes = []
        ok_ids = []
        fail_reasons = {}
        fail_samples = {}

        for tid in pending:
            reason = None

            # chamada API
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
                fail_samples.setdefault(reason, tid)
                continue

            if data is None:
                # 404 tratado separadamente
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            # API às vezes devolve lista
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

            detalhes.append(row)
            ok_ids.append(tid)

            if THROTTLE > 0:
                time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = total_pendentes - total_ok

        logger.info(
            "detail: processados neste ciclo: ok=%s, falhas=%s.",
            total_ok,
            total_fail,
        )

        if fail_reasons:
            logger.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                logger.info(
                    "detail:   - %s: %s tickets (exemplo ticket_id=%s)",
                    r,
                    c,
                    fail_samples.get(r),
                )

        if detalhes:
            upsert_details(conn, detalhes)
            logger.info(
                "detail: %s tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.",
                total_ok,
            )
        else:
            logger.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas."
            )

        # Se os IDs vieram do audit_recent_missing, remove todos de lá (ok + falha)
        if source == "audit":
            delete_from_missing(conn, pending)
            logger.info(
                "detail: %s tickets removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
                total_pendentes,
            )


if __name__ == "__main__":
    main()
