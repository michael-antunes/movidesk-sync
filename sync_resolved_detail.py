#!/usr/bin/env python3
import os
import time
import logging
import argparse
from typing import Iterable, Dict, Any, List, Optional

import psycopg2
import psycopg2.extras
import requests

LOG = logging.getLogger("sync_resolved_detail")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)7s  %(message)s",
)

MOVIDESK_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
THROTTLE = float(os.environ.get("MOVIDESK_THROTTLE", "0.25"))

# Campos que iremos coletar no Movidesk
SELECT_FIELDS = ",".join([
    "id",
    "status",
    "resolvedIn",
    "closedIn",
    "canceledIn",              # <- importante!
    "responsibleId",
    "responsibleName",
    "organizationId",
    "organizationName",
    "origin",
    "category",
    "urgency",
    "serviceFirstLevel",
    "serviceSecondLevel",
    "serviceThirdLevel",
])

UPSERT_SQL = """
INSERT INTO visualizacao_resolvidos.tickets_resolvidos (
  ticket_id, status,
  last_resolved_at, last_closed_at, last_cancelled_at,
  responsible_id, responsible_name,
  organization_id, organization_name,
  origin, category, urgency,
  service_first_level, service_second_level, service_third_level
) VALUES (
  %(ticket_id)s, %(status)s,
  %(last_resolved_at)s, %(last_closed_at)s, %(last_cancelled_at)s,
  %(responsible_id)s, %(responsible_name)s,
  %(organization_id)s, %(organization_name)s,
  %(origin)s, %(category)s, %(urgency)s,
  %(service_first_level)s, %(service_second_level)s, %(service_third_level)s
)
ON CONFLICT (ticket_id) DO UPDATE SET
  status               = EXCLUDED.status,
  last_resolved_at     = EXCLUDED.last_resolved_at,
  last_closed_at       = EXCLUDED.last_closed_at,
  last_cancelled_at    = EXCLUDED.last_cancelled_at,
  responsible_id       = EXCLUDED.responsible_id,
  responsible_name     = EXCLUDED.responsible_name,
  organization_id      = EXCLUDED.organization_id,
  organization_name    = EXCLUDED.organization_name,
  origin               = EXCLUDED.origin,
  category             = EXCLUDED.category,
  urgency              = EXCLUDED.urgency,
  service_first_level  = EXCLUDED.service_first_level,
  service_second_level = EXCLUDED.service_second_level,
  service_third_level  = EXCLUDED.service_third_level
"""

# ---------------------- Movidesk helpers ----------------------

session = requests.Session()
session.headers.update({"Content-Type": "application/json; charset=utf-8"})

def md_get_ticket(ticket_id: int) -> Dict[str, Any]:
    """
    Busca um ticket por ID usando OData (1 request por ID -> mais robusto).
    """
    params = {
        "token": TOKEN,
        "$select": SELECT_FIELDS,
        "$filter": f"id eq {ticket_id}",
        "$top": 1,
    }
    # NÃO usar $take (Movidesk não suporta) e NÃO colocar aspas em valores datetimetz do filtro.
    url = f"{MOVIDESK_BASE}/tickets"
    r = session.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    payload = r.json()
    if not payload:
        raise KeyError(f"Ticket {ticket_id} não retornou na API")
    return payload[0]

def norm_ts(v: Optional[str]) -> Optional[str]:
    """
    Normaliza campos datetime da API para None/ISO.
    A API pode retornar null, string vazia ou '0001-01-01T00:00:00Z' para 'vazio'.
    """
    if not v or v == "0001-01-01T00:00:00Z":
        return None
    return v  # psycopg2 aceita ISO 8601 e grava como timestamptz

def row_from_ticket(t: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticket_id": int(t["id"]),
        "status": t.get("status"),
        "last_resolved_at":  norm_ts(t.get("resolvedIn")),
        "last_closed_at":    norm_ts(t.get("closedIn")),
        "last_cancelled_at": norm_ts(t.get("canceledIn")),  # <- aqui!
        "responsible_id":    t.get("responsibleId"),
        "responsible_name":  t.get("responsibleName"),
        "organization_id":   t.get("organizationId"),
        "organization_name": t.get("organizationName"),
        "origin":            t.get("origin"),
        "category":          t.get("category"),
        "urgency":           t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
    }

# ---------------------- DB helpers ----------------------

def get_ids_to_fill_cancelled(cur) -> List[int]:
    """
    Se nenhum ID for passado na linha de comando, o script preenche
    todos os tickets CANCELADOS que ainda não têm last_cancelled_at.
    """
    cur.execute("""
        SELECT ticket_id
        FROM visualizacao_resolvidos.tickets_resolvidos
        WHERE status ILIKE 'canc%'                -- Canceled/Cancelled
          AND last_cancelled_at IS NULL
        ORDER BY ticket_id
        LIMIT 1000
    """)
    return [r[0] for r in cur.fetchall()]

def upsert_ticket(cur, row: Dict[str, Any]) -> None:
    cur.execute(UPSERT_SQL, row)

# ---------------------- main ----------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync detail (tickets_resolvidos) incluindo last_cancelled_at")
    p.add_argument("--ids", help="Lista de IDs separados por vírgula (opcional). Se não informar, busca no banco os cancelados sem data.")
    return p.parse_args()

def main():
    args = parse_args()

    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            if args.ids:
                ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
            else:
                ids = get_ids_to_fill_cancelled(cur)

            if not ids:
                LOG.info("Nenhum ID para processar.")
                return

            LOG.info("Processando %d ticket(s)...", len(ids))

            processed = 0
            for i, ticket_id in enumerate(ids, start=1):
                try:
                    t = md_get_ticket(ticket_id)
                    row = row_from_ticket(t)
                    upsert_ticket(cur, row)
                    processed += 1
                    if THROTTLE > 0:
                        time.sleep(THROTTLE)
                except Exception as e:
                    LOG.error("Falha no ticket %s: %s", ticket_id, e, exc_info=False)

                if i % 50 == 0:
                    conn.commit()
                    LOG.info("Checkpoint: %d/%d confirmados", i, len(ids))

            conn.commit()
            LOG.info("Finalizado. %d ticket(s) gravados/atualizados.", processed)

if __name__ == "__main__":
    main()
