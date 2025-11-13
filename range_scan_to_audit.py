import os, time, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN   = os.getenv("NEON_DSN")
PAGE_TOP = 100
LIMIT = int(os.getenv("RANGE_SCAN_LIMIT","400"))
THROTTLE = float(os.getenv("RANGE_SCAN_THROTTLE","0.25"))
if not TOKEN or not DSN: raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

def z(dt): return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
def to_dt(x):
    if not x: return None
    try: return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(timezone.utc)
    except: return None

def conn(): return psycopg2.connect(DSN)

def ensure_table():
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            create table if not exists visualizacao_resolvidos.range_scan_control(
              data_fim timestamptz not null,
              data_inicio timestamptz not null,
              ultima_data_validada timestamptz
            )
        """)
        cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
        if cur.fetchone()[0] == 0:
            cur.execute("""
                insert into visualizacao_resolvidos.range_scan_control(data_inicio,data_fim,ultima_data_validada)
                values (now(), timestamptz '2023-01-01 00:00:00+00', now())
            """)

def fetch(params):
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200: raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:300]}")
    return r.json() or []

def main():
    ensure_table()
    with conn() as c, c.cursor() as cur:
        cur.execute("select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio) from visualizacao_resolvidos.range_scan_control limit 1")
        data_inicio, data_fim, ultima = cur.fetchone()

    if ultima <= data_fim: return

    ids = []
    min_evt = None
    skip = 0
    while len(ids) < LIMIT:
        params = {
            "token": TOKEN,
            "$select": "id,baseStatus,resolvedIn,closedIn,canceledIn,lastUpdate",
            "$filter": f"lastUpdate ge {z(data_fim)} and lastUpdate le {z(ultima)}",
            "$orderby": "lastUpdate desc",
            "$top": PAGE_TOP,
            "$skip": skip
        }
        page = fetch(params)
        if not page: break
        for t in page:
            bs = (t.get("baseStatus") or "").strip()
            ev = None
            if bs in ("Resolved","Closed"):
                ev = to_dt(t.get("resolvedIn")) or to_dt(t.get("closedIn"))
            elif bs == "Canceled":
                ev = to_dt(t.get("canceledIn"))
            if not ev: continue
            if ev < data_fim or ev > ultima: continue
            try:
                tid = int(t.get("id"))
            except:
                continue
            if tid not in ids: ids.append(tid)
            if min_evt is None or ev < min_evt: min_evt = ev
            if len(ids) >= LIMIT: break
        if len(page) < PAGE_TOP: break
        skip += PAGE_TOP
        time.sleep(THROTTLE)

    missing = []
    with conn() as c, c.cursor() as cur:
        if ids:
            cur.execute("select ticket_id from visualizacao_resolvidos.tickets_resolvidos where ticket_id = any(%s)", (ids,))
            have = {r[0] for r in cur.fetchall()}
            missing = [i for i in ids if i not in have]
            cur.execute("select max(id) from visualizacao_resolvidos.audit_recent_run")
            rid = cur.fetchone()[0]
            if rid is None:
                cur.execute("""
                    insert into visualizacao_resolvidos.audit_recent_run
                      (window_start, window_end, total_api, missing_total, run_at, window_from, window_to, total_local, notes)
                    values (now(), now(), 0, 0, now(), %s, %s, 0, 'range-scan-backward') returning id
                """, (data_fim, ultima))
                rid = cur.fetchone()[0]
            if missing:
                execute_values(cur, """
                    insert into visualizacao_resolvidos.audit_recent_missing (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                """, [(rid, "tickets_resolvidos", m) for m in missing])
        if min_evt:
            nv = min_evt
        else:
            nv = data_fim
        cur.execute("update visualizacao_resolvidos.range_scan_control set ultima_data_validada = %s", (nv,))
        c.commit()

if __name__ == "__main__":
    main()
