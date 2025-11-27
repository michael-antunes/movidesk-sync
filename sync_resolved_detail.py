#!/usr/bin/env python
"""
Sincroniza detalhes dos tickets resolvidos da Movidesk
para a tabela visualizacao_resolvidos.tickets_resolvidos.

Fluxo:

1. Busca a lista de tickets pendentes em
   visualizacao_resolvidos.audit_recent_missing, usando:

   SELECT ticket_id
     FROM visualizacao_resolvidos.audit_recent_missing
    WHERE table_name = 'tickets_resolvidos'
    GROUP BY ticket_id
    ORDER BY MAX(run_id) DESC, ticket_id DESC
    LIMIT %s;

2. Se a consulta da audit der erro ou vier vazia, cai para
   um fallback em visualizacao_resolvidos.tickets_resolvidos:

   SELECT t.ticket_id
     FROM visualizacao_resolvidos.tickets_resolvidos t
     LEFT JOIN visualizacao_resolvidos.audit_ticket_watch w
       ON w.ticket_id = t.ticket_id
    WHERE t.last_update IS NULL
      AND w.ticket_id IS NULL
    ORDER BY t.ticket_id DESC
    LIMIT %s;

3. Para cada ticket, consulta o detalhe na API da Movidesk
   (GET /tickets/{id}?token=...).

4. Faz UPSERT na tabela tickets_resolvidos.

5. Registra falhas permanentes (ex.: not_found_404) em
   visualizacao_resolvidos.audit_ticket_watch.

6. Remove todos os tickets processados (ok + falha) de
   visualizacao_resolvidos.audit_recent_missing.
"""

import logging
import os
import time
from typing import Iterable, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests

LOG = logging.getLogger("detail")


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

API_BASE = "https://api.movidesk.com/public/v1"

# Secrets do GitHub Actions / ambiente
MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]
PG_DSN = os.environ["PG_DSN"]

# Pode deixar sem variável de ambiente que usa os defaults
BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE_SEC = float(os.getenv("DETAIL_THROTTLE_SEC", "0.25"))


# ---------------------------------------------------------------------------
# SQLs
# ---------------------------------------------------------------------------

# 1) Pega pendências a partir da audit (uma linha por ticket, mas pode ter
#    várias execuções de auditoria para o mesmo ticket, por isso o GROUP BY).
SQL_GET_PENDING_FROM_AUDIT = """
SELECT ticket_id
  FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
 GROUP BY ticket_id
 ORDER BY MAX(run_id) DESC, ticket_id DESC
 LIMIT %s;
"""

# 2) Fallback: tickets_resolvidos com last_update NULL que ainda não estão
#    marcados na audit_ticket_watch.
SQL_GET_PENDING_FROM_TICKETS = """
SELECT t.ticket_id
  FROM visualizacao_resolvidos.tickets_resolvidos t
  LEFT JOIN visualizacao_resolvidos.audit_ticket_watch w
    ON w.ticket_id = t.ticket_id
 WHERE t.last_update IS NULL
   AND w.ticket_id IS NULL
 ORDER BY t.ticket_id DESC
 LIMIT %s;
"""

# 3) UPSERT em tickets_resolvidos
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
) VALUES %s
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
    adicional_nome       = EXCLUDED.adicional_nome;
-- coluna adicionado_em_tabela fica por conta do DEFAULT do banco
"""

# 4) Remove tickets processados da audit_recent_missing
SQL_DELETE_FROM_AUDIT = """
DELETE FROM visualizacao_resolvidos.audit_recent_missing
 WHERE table_name = 'tickets_resolvidos'
   AND ticket_id = ANY(%s);
"""

# 5) Marca falhas permanentes na audit_ticket_watch
#    (PK é só ticket_id, então usamos ON CONFLICT (ticket_id)).
SQL_REGISTER_FAILURE = """
INSERT INTO visualizacao_resolvidos.audit_ticket_watch (
    table_name,
    ticket_id,
    last_seen_at
) VALUES (
    'tickets_resolvidos',
    %(ticket_id)s,
    now()
)
ON CONFLICT (ticket_id) DO UPDATE
   SET table_name = EXCLUDED.table_name,
       last_seen_at = EXCLUDED.last_seen_at;
"""


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def md_get(session: requests.Session, path_or_url: str, *, ok_404: bool = False) -> dict | None:
    """
    GET simples na API da Movidesk.

    Se ok_404=True, retorna None quando a API responder 404.
    """
    if path_or_url.startswith("http"):
        url = path_or_url
    else:
        url = f"{API_BASE.rstrip('/')}/{path_or_url.lstrip('/')}"

    params = {"token": MOVIDESK_TOKEN}

    resp = session.get(url, params=params, timeout=30)
    if resp.status_code == 404 and ok_404:
        return None

    resp.raise_for_status()
    return resp.json()


def get_pending_ticket_ids(conn, limit: int) -> List[int]:
    """
    Primeiro tenta pegar pendências em audit_recent_missing.
    Se der erro ou vier vazio, cai para o fallback em tickets_resolvidos.
    """
    # 1) Audit
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_GET_PENDING_FROM_AUDIT, (limit,))
            rows = cur.fetchall()
        ids = [r[0] for r in rows]
        if ids:
            LOG.info(
                "detail: %s tickets pendentes em visualizacao_resolvidos.audit_recent_missing (limite=%s).",
                len(ids),
                limit,
            )
            return ids
        else:
            LOG.info(
                "detail: nenhum ticket pendente em visualizacao_resolvidos.audit_recent_missing; usando fallback em tickets_resolvidos."
            )
    except Exception as exc:  # noqa: BLE001
        LOG.error(
            "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s). Caindo para tickets_resolvidos.",
            exc,
        )

    # 2) Fallback em tickets_resolvidos
    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING_FROM_TICKETS, (limit,))
        rows = cur.fetchall()
    ids = [r[0] for r in rows]
    if ids:
        LOG.info(
            "detail: %s tickets pendentes em visualizacao_resolvidos.tickets_resolvidos (limite=%s).",
            len(ids),
            limit,
        )
    else:
        LOG.info("detail: nenhum ticket pendente em visualizacao_resolvidos.tickets_resolvidos.")
    return ids


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """Marca o ticket na audit_ticket_watch para não ficar tentando para sempre."""
    with conn.cursor() as cur:
        cur.execute(SQL_REGISTER_FAILURE, {"ticket_id": ticket_id})
    conn.commit()
    LOG.warning("detail: ticket %s falhou: %s", ticket_id, reason)


def build_detail_row(ticket: dict) -> Tuple:
    """
    Converte o JSON do ticket Movidesk no tuple esperado pelo UPSERT.

    Campos de datas seguem a documentação:
    - resolvedIn  -> last_resolved_at
    - closedIn    -> last_closed_at
    - canceledIn  -> last_cancelled_at
    - lastUpdate  -> last_update
    """
    # Datas principais
    last_resolved_at = ticket.get("resolvedIn")
    last_closed_at = ticket.get("closedIn")
    last_cancelled_at = ticket.get("canceledIn")
    last_update = ticket.get("lastUpdate")

    # Campos de classificação
    origin = ticket.get("origin")
    category = ticket.get("category")
    urgency = ticket.get("urgency")

    # Serviços
    service_first_level = ticket.get("serviceFirstLevel")
    service_second_level = ticket.get("serviceSecondLevel")
    service_third_level = ticket.get("serviceThirdLevel")

    # Dono
    owner = ticket.get("owner") or {}
    owner_id = owner.get("id")
    owner_name = owner.get("businessName")
    owner_team_name = owner.get("team")

    # Organização
    org = ticket.get("organization") or {}
    organization_id = org.get("id")
    organization_name = org.get("businessName")

    # Outros
    subject = ticket.get("subject")

    # Exemplo de campo adicional (ajuste o id conforme seus customFields)
    adicional_nome = None
    custom_fields = ticket.get("customFields") or []
    for cf in custom_fields:
        if cf.get("id") == "adicional_nome":
            adicional_nome = cf.get("value")
            break

    return (
        int(ticket["id"]),
        ticket.get("status"),
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
    )


def upsert_details(conn, rows: Iterable[Tuple]) -> None:
    rows = list(rows)
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, SQL_UPSERT_DETAILS, rows)
    conn.commit()


def delete_from_audit(conn, ticket_ids: List[int]) -> None:
    if not ticket_ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_FROM_AUDIT, (ticket_ids,))
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging()
    LOG.info("detail: início da sincronização (batch=%s).", BATCH_SIZE)

    conn = psycopg2.connect(PG_DSN)
    session = requests.Session()

    try:
        pending = get_pending_ticket_ids(conn, BATCH_SIZE)
        if not pending:
            LOG.info("detail: nada para processar, encerrando.")
            return

        detalhes: List[Tuple] = []
        ok_ids: List[int] = []

        total_ok = 0
        total_fail = 0
        fail_reasons: dict[str, int] = {}
        fail_sample: dict[str, int] = {}

        for ticket_id in pending:
            try:
                ticket = md_get(session, f"tickets/{ticket_id}", ok_404=True)
            except Exception as exc:  # noqa: BLE001
                reason = f"erro_http_{type(exc).__name__}"
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_sample:
                    fail_sample[reason] = ticket_id
                LOG.exception("detail: erro HTTP ao buscar ticket %s: %s", ticket_id, exc)
                continue

            if ticket is None:
                # 404 na API
                reason = "not_found_404"
                total_fail += 1
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_sample:
                    fail_sample[reason] = ticket_id
                register_ticket_failure(conn, ticket_id, reason)
                continue

            try:
                row = build_detail_row(ticket)
            except Exception as exc:  # noqa: BLE001
                reason = "parse_error"
                total_fail += 1
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_sample:
                    fail_sample[reason] = ticket_id
                LOG.exception("detail: erro ao montar linha para ticket %s: %s", ticket_id, exc)
                continue

            detalhes.append(row)
            ok_ids.append(ticket_id)
            total_ok += 1

            # Respeita limite de requisições
            if THROTTLE_SEC:
                time.sleep(THROTTLE_SEC)

        # Persiste o que deu certo
        if detalhes:
            upsert_details(conn, detalhes)

        # Remove todos (ok + falha) da audit_recent_missing
        if pending:
            delete_from_audit(conn, pending)

        LOG.info(
            "detail: processados neste ciclo: ok=%s, falhas=%s.",
            total_ok,
            total_fail,
        )
        if fail_reasons:
            LOG.info("detail: razões de falha neste ciclo:")
            for reason, qty in fail_reasons.items():
                sample_id = fail_sample.get(reason)
                if sample_id is not None:
                    LOG.info(
                        "detail:   - %s: %s tickets (exemplo ticket_id=%s)",
                        reason,
                        qty,
                        sample_id,
                    )
                else:
                    LOG.info("detail:   - %s: %s tickets", reason, qty)

    finally:
        conn.close()
        session.close()


if __name__ == "__main__":
    main()
