#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
from typing import List, Tuple, Iterable, Optional

import psycopg2
from psycopg2.extras import execute_values
import requests

# ============================================================
# CONFIGURAÇÃO GERAL
# ============================================================

LOG = logging.getLogger("detail")

API_BASE = "https://api.movidesk.com/public/v1"

# Permite os dois nomes usados nos workflows
MOVIDESK_TOKEN = (
    os.getenv("MOVIDESK_TOKEN")
    or os.getenv("MOVIDESK_API_TOKEN")
)

DSN = (
    os.getenv("PG_DSN")
    or os.getenv("NEON_DSN")
)

BATCH_SIZE = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE_SEC = float(os.getenv("THROTTLE_SEC", "0.25"))

if not MOVIDESK_TOKEN or not DSN:
    raise RuntimeError(
        "Defina MOVIDESK_TOKEN ou MOVIDESK_API_TOKEN e PG_DSN ou NEON_DSN nas variáveis de ambiente."
    )

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})

# ============================================================
# CONSULTAS SQL USANDO NOMES EXATOS DO SEU BANCO
# ============================================================

# Busca pendências na audit_recent_missing, ignorando já registrados em audit_ticket_watch
SQL_GET_PENDING = """
    SELECT a.ticket_id
      FROM visualizacao_resolvidos.audit_recent_missing a
 LEFT JOIN visualizacao_resolvidos.audit_ticket_watch w
        ON w.ticket_id = a.ticket_id
     WHERE a.table_name = 'tickets_resolvidos'
       AND w.ticket_id IS NULL
  GROUP BY a.ticket_id
  ORDER BY MAX(a.run_id) DESC, a.ticket_id DESC
     LIMIT %s
"""

# Remove tickets processados da audit_recent_missing
SQL_DELETE_FROM_AUDIT = """
    DELETE FROM visualizacao_resolvidos.audit_recent_missing
     WHERE table_name = 'tickets_resolvidos'
       AND ticket_id = ANY(%s)
"""

# Inserção de falhas em audit_ticket_watch
SQL_INSERT_WATCH = """
    INSERT INTO visualizacao_resolvidos.audit_ticket_watch (ticket_id, table_name, last_seen_at)
    VALUES (%s, 'tickets_resolvidos', now())
    ON CONFLICT (ticket_id)
        DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at;
"""

# Upsert completo em tickets_resolvidos (todos campos reais)
SQL_UPSERT_TICKETS = """
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
        adicional_nome       = EXCLUDED.adicional_nome;
"""

# ============================================================
# FUNÇÕES DE APOIO
# ============================================================

def md_get(path: str, params: Optional[dict] = None, ok_404: bool = False):
    """Chama API Movidesk com retry leve"""
    url = f"{API_BASE}/{path}"
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


def registrar_falha(conn, ticket_id: int) -> None:
    """Registra falha (404, erro, etc) em audit_ticket_watch"""
    with conn.cursor() as cur:
        cur.execute(SQL_INSERT_WATCH, (ticket_id,))
    conn.commit()


def obter_pendentes(conn, limite: int) -> List[int]:
    """Busca IDs de tickets pendentes, ignorando já observados"""
    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING, (limite,))
        ids = [r[0] for r in cur.fetchall()]
        LOG.info("detail: %s tickets pendentes (limite=%s)", len(ids), limite)
        return ids


def montar_tupla(ticket: dict) -> Tuple:
    """Monta a linha de inserção/atualização com base nos campos do Movidesk"""
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


def upsert_tickets(conn, rows: Iterable[Tuple]) -> None:
    """Upsert no tickets_resolvidos"""
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, SQL_UPSERT_TICKETS, rows, page_size=200)
    conn.commit()


def deletar_audit(conn, ids: List[int]) -> None:
    """Remove tickets processados da audit_recent_missing"""
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_FROM_AUDIT, (ids,))
    conn.commit()

# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    with psycopg2.connect(DSN) as conn:
        pendentes = obter_pendentes(conn, BATCH_SIZE)
        if not pendentes:
            LOG.info("detail: nenhum ticket pendente para atualização de detalhes.")
            return

        detalhes: List[Tuple] = []
        falhas: List[int] = []

        for tid in pendentes:
            data = md_get(
                f"tickets/{tid}",
                params={"$expand": "clients,createdBy,owner,actions,customFields"},
                ok_404=True
            )

            if data is None:
                falhas.append(tid)
                registrar_falha(conn, tid)
                continue

            ticket = data[0] if isinstance(data, list) else data
            if not ticket.get("id"):
                falhas.append(tid)
                registrar_falha(conn, tid)
                continue

            detalhes.append(montar_tupla(ticket))
            time.sleep(THROTTLE_SEC)

        if detalhes:
            upsert_tickets(conn, detalhes)
            LOG.info("detail: %s tickets upsertados em visualizacao_resolvidos.tickets_resolvidos.", len(detalhes))

        deletar_audit(conn, pendentes)
        LOG.info("detail: %s tickets removidos de visualizacao_resolvidos.audit_recent_missing.", len(pendentes))

        if falhas:
            LOG.warning("detail: %s tickets retornaram 404 (exemplo ticket_id=%s)", len(falhas), falhas[0])


if __name__ == "__main__":
    main()
