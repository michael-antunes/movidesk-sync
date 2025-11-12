#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, sys, math
import requests
import psycopg2, psycopg2.extras
from datetime import datetime, timezone, timedelta

API_TOKEN = os.getenv("MOVIDESK_API_TOKEN") or os.getenv("MOVIDESK_TOKEN")
BASE_URL  = "https://api.movidesk.com/public/v1"
DSN       = os.getenv("NEON_DSN")

BATCH_SIZE   = int(os.getenv("ACTIONS_BATCH_SIZE", "10"))
THROTTLE_SEC = float(os.getenv("ACTIONS_THROTTLE_SEC", "0.7"))  # respiro entre requisições
HARD_RETRY   = int(os.getenv("ACTIONS_RETRY", "3"))

# ---------- HTTP helper com backoff e tratamento de 429 ----------
def req(path, params, retry=HARD_RETRY):
    params = dict(params or {})
    params["token"] = API_TOKEN
    url = f"{BASE_URL}{path}"
    for attempt in range(retry+1):
        r = requests.get(url, params=params, timeout=60)
        # 2xx
        if 200 <= r.status_code < 300:
            if not r.text:
                return None
            try:
                return r.json()
            except Exception:
                return None
        # 429 (limite)
        if r.status_code == 429:
            wait = int(r.headers.get("retry-after", "60"))
            time.sleep(max(wait, 5))
            continue
        # outros 4xx/5xx: pequena pausa + retry
        if attempt < retry:
            time.sleep(2*(attempt+1))
            continue
        r.raise_for_status()

# ---------- DB ----------
def get_conn():
    return psycopg2.connect(DSN)

UPSERT = """
INSERT INTO visualizacao_resolvidos.resolvidos_acoes (ticket_id, acoes, updated_at)
VALUES (%s, %s::jsonb, now())
ON CONFLICT (ticket_id) DO UPDATE
   SET acoes = EXCLUDED.acoes,
       updated_at = now();
"""

AUDIT_HIT = """
INSERT INTO visualizacao_resolvidos.audit_recent_run (table_name, ticket_id, audit_recent_run)
VALUES ('resolvidos_acoes', %s, now())
ON CONFLICT DO NOTHING;
"""

AUDIT_MISS = """
INSERT INTO visualizacao_resolvidos.audit_recent_missing (table_name, ticket_id, audit_recent_missing)
VALUES ('resolvidos_acoes', %s, now())
ON CONFLICT DO NOTHING;
"""

def upsert_rows(conn, rows):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=50)
    conn.commit()

def audit_hit(conn, tid):
    try:
        with conn.cursor() as cur:
            cur.execute(AUDIT_HIT, (tid,))
        conn.commit()
    except Exception:
        conn.rollback()

def audit_miss(conn, tid):
    try:
        with conn.cursor() as cur:
            cur.execute(AUDIT_MISS, (tid,))
        conn.commit()
    except Exception:
        conn.rollback()

# ---------- seleção de 10 tickets a partir de tickets_resolvidos ----------
CANDIDATES_SQL = """
WITH base AS (
  SELECT t.ticket_id
  FROM visualizacao_resolvidos.tickets_resolvidos t
  LEFT JOIN visualizacao_resolvidos.resolvidos_acoes a
         ON a.ticket_id = t.ticket_id
  WHERE a.ticket_id IS NULL     -- ainda não tem ações salvas
  ORDER BY t.ticket_id DESC     -- fallback robusto (colunas de data variam)
  LIMIT %s
)
SELECT ticket_id FROM base ORDER BY ticket_id DESC;
"""

def pick_candidates(conn, limit=BATCH_SIZE):
    with conn.cursor() as cur:
        cur.execute(CANDIDATES_SQL, (limit,))
        return [r[0] for r in cur.fetchall()]

# ---------- coleta de ações JSON + HTML ----------
def fetch_actions_for_ticket(ticket_id: int):
    """
    Busca o ticket com as ações (JSON). Para HTML das ações,
    usa o endpoint /tickets/htmldescription por actionId.
    """
    # 1) lista de ações (JSON) – atenção: htmlDescription NÃO vem na listagem.
    select_fields = "id,lastUpdate,actionCount"
    expand = (
        "actions("
        "$select=id,isPublic,description,createdDate,origin;"
        "$expand=timeAppointments($select=id,activity,date,periodStart,periodEnd,workTime,accountedTime,workTypeName,createdBy,createdByTeam),"
        "attachments($select=fileName,path,createdDate)"
        ")"
    )

    data = req(
        "/tickets",
        {
            "$select": select_fields,
            "$expand": expand,
            "id": str(ticket_id),
            "includeDeletedItems": "true",
        },
    )

    if not data:
        return None  # não encontrado

    # a API devolve {} quando usa id=...; normalizamos para um dict
    if isinstance(data, list) and data:
        item = data[0]
    elif isinstance(data, dict):
        item = data
    else:
        return {"ticket_id": ticket_id, "actions": []}

    actions = item.get("actions") or []

    # 2) HTML das ações (campo "description" em HTML)
    # Doc: /tickets/htmldescription?id=<ticket>&actionId=<id>  -> {"id":1,"description":"<html>..."}
    rich = []
    for a in actions:
        action_id = a.get("id")
        html = None
        try:
            # evitar estourar limite
            time.sleep(THROTTLE_SEC)
            html_obj = req("/tickets/htmldescription", {"id": str(ticket_id), "actionId": str(action_id)})
            if isinstance(html_obj, dict):
                html = html_obj.get("description")
        except Exception:
            html = None

        a2 = dict(a)
        if html is not None:
            a2["html"] = html  # adiciona o HTML ao JSON da ação
        rich.append(a2)

    return {
        "ticket_id": ticket_id,
        "actions": rich,
        "lastUpdate": item.get("lastUpdate"),
        "actionCount": item.get("actionCount", len(rich)),
    }

# ---------- main ----------
def main():
    if not API_TOKEN:
        print("MOVIDESK_API_TOKEN não configurado", file=sys.stderr)
        sys.exit(2)
    conn = get_conn()

    # pega 10 candidatos da tabela tickets_resolvidos
    candidates = pick_candidates(conn, BATCH_SIZE)
    if not candidates:
        print("Nenhum ticket pendente em tickets_resolvidos.")
        return

    rows = []
    for tid in candidates:
        try:
            # respiro entre tickets para respeitar rate limit
            time.sleep(THROTTLE_SEC)
            obj = fetch_actions_for_ticket(tid)
            if not obj:
                audit_miss(conn, tid)
                continue

            acoes_json = json.dumps(obj["actions"], ensure_ascii=False)
            rows.append((tid, acoes_json))
            audit_hit(conn, tid)

        except requests.HTTPError as e:
            # se der 404/400 marcamos como miss para investigação
            audit_miss(conn, tid)
            print(f"[WARN] ticket {tid} => HTTP {e}", file=sys.stderr)
        except Exception as e:
            audit_miss(conn, tid)
            print(f"[WARN] ticket {tid} => {e}", file=sys.stderr)

    if rows:
        upsert_rows(conn, rows)

    conn.close()

if __name__ == "__main__":
    main()
