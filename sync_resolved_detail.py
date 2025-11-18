import os
import time
import json
import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")
BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})

def md_get(path_or_full, params=None, ok_404=False):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN
    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or {}
    if ok_404 and r.status_code == 404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or {}
    r.raise_for_status()

SQL_GET_PENDING = """
select ticket_id
  from visualizacao_resolvidos.audit_recent_missing
 where table_name = 'tickets_resolvidos'
 group by ticket_id
 order by max(run_id) desc, ticket_id desc
 limit %s
"""

SQL_DELETE_MISSING = """
delete from visualizacao_resolvidos.audit_recent_missing
 where table_name = 'tickets_resolvidos'
   and ticket_id = any(%s)
"""

def register_ticket_failure(conn, ticket_id, reason):
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from visualizacao_resolvidos.audit_ticket_watch
             where table_name = 'tickets_resolvidos'
               and ticket_id = %s
            """,
            (ticket_id,),
        )
        cur.execute(
            """
            insert into visualizacao_resolvidos.audit_ticket_watch(table_name, ticket_id, last_seen_at)
            values ('tickets_resolvidos', %s, now())
            """,
            (ticket_id,),
        )
    conn.commit()
