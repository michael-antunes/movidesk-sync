#!/usr/bin/env python
import os
import time
import logging
from typing import List, Tuple, Iterable, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values

# --------------------------------------------------------------------
# Configuração básica
# --------------------------------------------------------------------

LOG = logging.getLogger("detail")

API_BASE = "https://api.movidesk.com/public/v1"

# Token: aceita MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN
MOVIDESK_TOKEN = (
    os.getenv("MOVIDESK_TOKEN")
    or os.getenv("MOVIDESK_API_TOKEN")
)

# DSN: aceita PG_DSN ou NEON_DSN (compatível com o que você já tinha)
DSN = (
    os.getenv("PG_DSN")
    or os.getenv("NEON_DSN")
)

BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE_SEC = float(os.getenv("THROTTLE_SEC", "0.25"))

if not MOVIDESK_TOKEN or not DSN:
    raise RuntimeError(
        "Defina MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN "
        "e PG_DSN ou NEON_DSN nas variáveis de ambiente."
    )

# sessão HTTP reutilizável
S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})

# --------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------

# Pendentes em audit_recent_missing (consulta principal)
SQL_GET_PENDING_AUDIT = """
    SELECT ticket_id
      FROM visualizacao_resolvidos.audit_recent_missing
     WHERE table_name = 'tickets_resolvidos'
     GROUP BY ticket_id
     ORDER BY MAX(run_id) DESC, ticket_id DESC
     LIMIT %s
"""

# Fallback: procurar tickets já existentes na tabela de detalhes
# que ainda estão sem alguma das datas importantes
SQL_GET_PENDING_FALLBACK = """
    SELECT ticket_id
      FROM visualizacao_resolvidos.tickets_resolvidos
     WHERE status IN ('Resolvido', 'Fechado', 'Cancelado')
       AND (
              last_update IS NULL
           OR (status = 'Resolvido' AND last_resolved_at IS NULL)
           OR (status = 'Fechado'   AND last_closed_at   IS NULL)
           OR (status = 'Cancelado' AND last_cancelled_at IS NULL)
       )
     ORDER BY ticket_id DESC
     LIMIT %s
"""

SQL_DELETE_MISSING = """
    DELETE FROM visualizacao_resolvidos.audit_recent_missing
     WHERE table_name = 'tickets_resolvidos'
       AND ticket_id = ANY(%s)
"""

# --------------------------------------------------------------------
# Funções auxiliares
# --------------------------------------------------------------------


def md_get(path_or_full: str,
           params: Optional[dict] = None,
           ok_404: bool = False):
    """
    Chama a API do Movidesk com retry básico em 429/5xx.
    Se ok_404=True, retorna None em caso de 404.
    """
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = MOVIDESK_TOKEN

    for attempt in (1, 2):
        r = S.get(url, params=p, timeout=60)
        if r.status_code == 200:
            return r.json() or {}
        if ok_404 and r.status_code == 404:
            return None
        if r.status_code in (429, 500, 502, 503, 504) and attempt == 1:
            time.sleep(1.5)
            continue
        r.raise_for_status()


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Marca o ticket em audit_ticket_watch.
    Usa ON CONFLICT para não estourar unique constraint.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO visualizacao_resolvidos.audit_ticket_watch (
                ticket_id, table_name, last_seen_at
            )
            VALUES (%s, 'tickets_resolvidos', now())
            ON CONFLICT (ticket_id) DO UPDATE
               SET table_name = EXCLUDED.table_name,
                   last_seen_at = EXCLUDED.last_seen_at
            """,
            (ticket_id,),
        )
    conn.commit()


def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Busca ids pendentes primeiro em audit_recent_missing.
    Se der erro na consulta (tipo SELECT DISTINCT / ORDER BY),
    cai para o fallback em tickets_resolvidos.
    """
    if limit is None or limit <= 0:
        limit = BATCH_SIZE

    with conn.cursor() as cur:
        try:
            cur.execute(SQL_GET_PENDING_AUDIT, (limit,))
            rows = cur.fetchall()
            ids = [r[0] for r in rows]
            LOG.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
                len(ids),
                limit,
            )
            return ids
        except Exception as e:
            LOG.error(
                "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s). "
                "Caindo para tickets_resolvidos.",
                e,
            )

        # fallback
        cur.execute(SQL_GET_PENDING_FALLBACK, (limit,))
        rows = cur.fetchall()
        ids = [r[0] for r in rows]
        LOG.info(
            "detail: %s tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
            len(ids),
            limit,
        )
        return ids


def build_detail_row(ticket: dict) -> Tuple:
    """
    Constrói a tupla com as colunas da tabela tickets_resolvidos,
    exceto adicionado_em_tabela (que é gerado pelo DEFAULT do banco).
    """
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


def upsert_details(conn, rows: Iterable[Tuple]) -> None:
    """
    Faz o upsert em visualizacao_resolvidos.tickets_resolvidos.
    Não toca em adicionado_em_tabela (DEFAULT controla).
    """
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


def delete_from_missing(conn, ids: List[int]) -> None:
    """
    Remove os tickets processados (ok + falha) de audit_recent_missing.
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_MISSING, (ids,))
    conn.commit()


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)

        if not pending:
            LOG.info("detail: nenhum ticket pendente para atualização de detalhes.")
            return

        LOG.info(
            "detail: %s tickets pendentes para atualização de detalhes (limite=%s).",
            total_pendentes,
            BATCH_SIZE,
        )

        detalhes: List[Tuple] = []
        fail_reasons: dict = {}
        fail_samples: dict = {}

        for tid in pending:
            reason = None

            # ------------------- chamada API -------------------
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

            # 404 da própria API
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

            # Monta linha para o upsert
            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                fail_samples.setdefault(reason, tid)
                continue

            detalhes.append(row)
            time.sleep(THROTTLE_SEC)

        total_ok = len(detalhes)
        total_fail = total_pendentes - total_ok

        LOG.info(
            "detail: processados neste ciclo: ok=%s, falhas=%s.",
            total_ok,
            total_fail,
        )

        if fail_reasons:
            LOG.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                LOG.info(
                    "detail:   - %s: %s tickets (exemplo ticket_id=%s)",
                    r,
                    c,
                    fail_samples.get(r),
                )

        if detalhes:
            upsert_details(conn, detalhes)
            LOG.info(
                "detail: %s tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.",
                total_ok,
            )
        else:
            LOG.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas."
            )

        # Sempre remover os pendentes da audit (tanto ok quanto falha)
        delete_from_missing(conn, pending)
        LOG.info(
            "detail: %s tickets removidos de visualizacao_resolvidos.audit_recent_missing (ok + falhas).",
            total_pendentes,
        )


if __name__ == "__main__":
    main()
