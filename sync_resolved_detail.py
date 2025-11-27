import os
import sys
import time
import logging
import json
import argparse
from typing import Iterable, Dict, Any, List, Optional
from typing import Any, Dict, List, Optional

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

TOKEN = os.environ.get("MOVIDESK_TOKEN", "")
DSN   = os.environ.get("NEON_DSN", "")
THROTTLE = float(os.environ.get("MOVIDESK_THROTTLE", "0.20"))  # segundos entre chamadas

session = requests.Session()
session.headers.update({"Content-Type": "application/json; charset=utf-8"})
session.headers.update({"Accept": "application/json"})

CANCEL_WORDS = {"canceled", "cancelled", "cancelado", "cancelada"}

def md_get_ticket(ticket_id: int) -> Dict[str, Any]:
# --------------------------- util ---------------------------

def norm_ts(v: Optional[str]) -> Optional[str]:
    """
    Busca um ticket por ID usando OData (1 request por ID -> mais robusto).
    Movidesk normalmente devolve ISO-8601 com offset (+00:00).
    Para o DB (timestamptz) podemos mandar a string ISO direto.
    """
    if not v:
        return None
    s = str(v).strip()
    return s or None

def log(msg: str) -> None:
    print(msg, flush=True)

# --------------------------- Movidesk ---------------------------

def md_get_ticket_with_history(ticket_id: int) -> Dict[str, Any]:
    """
    Busca 1 ticket por id com campos principais + histories resumido.
    """
    params = {
        "token": TOKEN,
        "$select": SELECT_FIELDS,
        "$filter": f"id eq {ticket_id}",
        "$top": 1,
        "$select": ",".join([
            "id","status",
            "resolvedIn","closedIn","canceledIn","cancelledIn",
            "responsibleId","responsibleName",
            "organizationId","organizationName",
            "origin","category","urgency",
            "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
        ]),
        "$expand": "histories($select=createdDate,field,oldValue,newValue,description)"
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
    data = r.json()
    if not data:
        raise KeyError(f"Ticket {ticket_id} não encontrado")
    return data[0]

def norm_ts(v: Optional[str]) -> Optional[str]:
def derive_cancelled_from_history(t: Dict[str, Any]) -> Optional[str]:
    """
    Normaliza campos datetime da API para None/ISO.
    A API pode retornar null, string vazia ou '0001-01-01T00:00:00Z' para 'vazio'.
    Quando o campo cancelado não vem direto, inferimos pelo histórico.
    """
    if not v or v == "0001-01-01T00:00:00Z":
        return None
    return v  # psycopg2 aceita ISO 8601 e grava como timestamptz
    hist = t.get("histories") or []
    for h in hist:
        field = (h.get("field") or "").strip().lower()
        newv  = (h.get("newValue") or "").strip().lower()
        desc  = (h.get("description") or "").strip().lower()

        # troca de status / situação para cancelado
        if field in {"status", "situation", "situação"} and newv in CANCEL_WORDS:
            return norm_ts(h.get("createdDate"))

        # alguns ambientes só deixam a palavra no description
        if any(w in desc for w in CANCEL_WORDS):
            return norm_ts(h.get("createdDate"))

    return None

def extract_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    resolved  = norm_ts(t.get("resolvedIn"))
    closed    = norm_ts(t.get("closedIn"))
    cancelled = norm_ts(t.get("canceledIn")) or norm_ts(t.get("cancelledIn"))
    if not cancelled:
        cancelled = derive_cancelled_from_history(t)

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

        "last_resolved_at": resolved,
        "last_closed_at":   closed,
        "last_cancelled_at": cancelled,

        "responsible_id":   t.get("responsibleId"),
        "responsible_name": t.get("responsibleName"),
        "organization_id":  t.get("organizationId"),
        "organization_name": t.get("organizationName"),
        "origin":            t.get("origin"),
        "category":          t.get("category"),
        "urgency":           t.get("urgency"),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
    }

# ---------------------- DB helpers ----------------------
# --------------------------- DB ---------------------------

def get_conn():
    if not DSN:
        raise RuntimeError("NEON_DSN não informado")
    return psycopg2.connect(DSN)

def get_ids_to_fill_cancelled(cur) -> List[int]:
def ensure_schema(cur) -> None:
    """
    Se nenhum ID for passado na linha de comando, o script preenche
    todos os tickets CANCELADOS que ainda não têm last_cancelled_at.
    Garante colunas essenciais (não mexe nas geradas).
    """
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS visualizacao_resolvidos;
        ALTER TABLE visualizacao_resolvidos.tickets_resolvidos
            ADD COLUMN IF NOT EXISTS last_cancelled_at timestamptz;
    """)

def ids_pendentes_cancelled(cur, limit: int) -> List[int]:
    cur.execute(f"""
        SELECT ticket_id
        FROM visualizacao_resolvidos.tickets_resolvidos
        WHERE status ILIKE 'canc%'                -- Canceled/Cancelled
          AND last_cancelled_at IS NULL
        WHERE last_cancelled_at IS NULL
          AND lower(status) IN ('canceled','cancelled','cancelado','cancelada')
        ORDER BY ticket_id
        LIMIT 1000
        LIMIT {int(limit)};
    """)
    return [r[0] for r in cur.fetchall()]

def upsert_ticket(cur, row: Dict[str, Any]) -> None:
    cur.execute(UPSERT_SQL, row)

# ---------------------- main ----------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync detail (tickets_resolvidos) incluindo last_cancelled_at")
    p.add_argument("--ids", help="Lista de IDs separados por vírgula (opcional). Se não informar, busca no banco os cancelados sem data.")
    """
    Atualiza campos de datas. Não sobrescreve com NULL.
    """
    cur.execute("""
        UPDATE visualizacao_resolvidos.tickets_resolvidos
           SET last_resolved_at  = COALESCE(%s, last_resolved_at),
               last_closed_at    = COALESCE(%s, last_closed_at),
               last_cancelled_at = COALESCE(%s, last_cancelled_at)
         WHERE ticket_id = %s
    """, (
        row["last_resolved_at"],
        row["last_closed_at"],
        row["last_cancelled_at"],
        row["ticket_id"],
    ))

# --------------------------- CLI / main ---------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Preenche last_cancelled_at (e datas) em tickets_resolvidos")
    p.add_argument("--ids", help="lista de IDs separados por vírgula (pula a seleção por status)", default="")
    p.add_argument("--limit", type=int, default=1000, help="limite de pendentes por execução")
    p.add_argument("--throttle", type=float, default=THROTTLE, help="intervalo entre chamadas à API em segundos")
    return p.parse_args()

def main():
    if not TOKEN:
        print("MOVIDESK_TOKEN não informado", file=sys.stderr)
        sys.exit(1)

    args = parse_args()
    throttle = max(0.0, float(args.throttle))

    with psycopg2.connect(DSN) as conn:
    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            ensure_schema(cur)

            if args.ids:
                ids = [int(x.strip()) for x in args.ids.split(",") if x.strip()]
                ids = [int(x) for x in args.ids.split(",") if x.strip()]
            else:
                ids = get_ids_to_fill_cancelled(cur)
                ids = ids_pendentes_cancelled(cur, args.limit)

            if not ids:
                LOG.info("Nenhum ID para processar.")
                log("Nada pendente para preencher.")
                return

            LOG.info("Processando %d ticket(s)...", len(ids))
            log(f"Processando {len(ids)} tickets...")
            ok = 0
            miss = 0

            processed = 0
            for i, ticket_id in enumerate(ids, start=1):
            for i, tid in enumerate(ids, 1):
                try:
                    t = md_get_ticket(ticket_id)
                    row = row_from_ticket(t)
                    t = md_get_ticket_with_history(tid)
                    row = extract_ticket_row(t)
                    upsert_ticket(cur, row)
                    processed += 1
                    if THROTTLE > 0:
                        time.sleep(THROTTLE)
                    ok += 1

                except Exception as e:
                    LOG.error("Falha no ticket %s: %s", ticket_id, e, exc_info=False)
                    miss += 1
                    log(f"[WARN] ticket {tid}: {e}")

                if i % 50 == 0:
                    conn.commit()
                    LOG.info("Checkpoint: %d/%d confirmados", i, len(ids))
                if throttle > 0:
                    time.sleep(throttle)

            conn.commit()
            LOG.info("Finalizado. %d ticket(s) gravados/atualizados.", processed)
            log(f"Concluído. Atualizados: {ok} | Falhas: {miss}")

if __name__ == "__main__":
    main()
