import os, time, requests, psycopg2
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]
BASE  = "https://api.movidesk.com/public/v1"
SCHEMA = "visualizacao_resolvidos"
RESOLVED = {"resolved","closed","resolvido","fechado"}
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))

def api_get(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(min(60, 2**i)); continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def ensure_structure():
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"create schema if not exists {SCHEMA};")
    cur.execute(f"""
        create table if not exists {SCHEMA}.tickets_resolvidos(
          ticket_id integer primary key,
          status    text not null
        );
    """)
    cur.execute(f"""
        create table if not exists {SCHEMA}.sync_control(
          name text primary key,
          last_update timestamp not null
        );
    """)
    cur.execute(f"""
        insert into {SCHEMA}.sync_control(name,last_update)
        values ('tr_lastresolved', now() - interval '60 days')
        on conflict (name) do nothing;
    """)
    cur.close(); conn.close()

def get_cursor():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select last_update from {SCHEMA}.sync_control where name='tr_lastresolved'")
    row=cur.fetchone(); cur.close(); conn.close()
    dt=row[0] if row else datetime.now(timezone.utc) - timedelta(days=60)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt

def set_cursor(ts):
    if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"update {SCHEMA}.sync_control set last_update=%s where name='tr_lastresolved'", (ts,))
    cur.close(); conn.close()

def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def list_ids_resolved_since(cursor_dt):
    ids=set(); top=500; skip=0
    conds = " or ".join([f"tolower(s/status) eq '{s}'" for s in RESOLVED])
    filt  = f"statusHistories/any(s: s/changedDate ge {iso(cursor_dt)} and ({conds}))"
    while True:
        p={"token":TOKEN,"$select":"id","$filter":filt,"$top":top,"$skip":skip}
        batch = api_get(f"{BASE}/tickets/past", p) or []
        if not batch: break
        for t in batch:
            tid = t.get("id")
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch) < top: break
        skip += top
    return list(ids)

def list_ids_reopened_since(cursor_dt):
    ids=set(); top=500; skip=0
    conds = " and ".join([f"tolower(s/status) ne '{s}'" for s in RESOLVED])
    filt  = f"statusHistories/any(s: s/changedDate ge {iso(cursor_dt)} and {conds})"
    while True:
        p={"token":TOKEN,"$select":"id","$filter":filt,"$top":top,"$skip":skip}
        batch = api_get(f"{BASE}/tickets/past", p) or []
        if not batch: break
        for t in batch:
            tid = t.get("id")
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch) < top: break
        skip += top
    return list(ids)

def fetch_ticket_min(ticket_id):
    p={'token':TOKEN,'id':ticket_id,'$select':'id,status,baseStatus,lastUpdate'}
    try:
        r = api_get(f"{BASE}/tickets", p)
        if isinstance(r, list): return r[0] if r else None
        return r
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    try:
        r = api_get(f"{BASE}/tickets/past", p)
        if isinstance(r, list): return r[0] if r else None
        return r
    except:
        return None

def is_resolved(t):
    st=(t.get("status") or "").lower()
    bs=(t.get("baseStatus") or "")
    if bs in ("Resolved","Closed"): return True
    return st in RESOLVED

def upsert_rows(items):
    if not items: return
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    for tid, st in items:
        cur.execute(f"""
            insert into {SCHEMA}.tickets_resolvidos(ticket_id, status)
            values (%s,%s)
            on conflict (ticket_id) do update set status=excluded.status;
        """,(tid, st))
    cur.close(); conn.close()

def delete_ids(ids):
    if not ids: return
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"delete from {SCHEMA}.tickets_resolvidos where ticket_id = any(%s)", (ids,))
    cur.close(); conn.close()

def current_index_ids():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id from {SCHEMA}.tickets_resolvidos")
    data=[r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return set(data)

def run():
    ensure_structure()
    cursor_dt=get_cursor()
    new_ids=list_ids_resolved_since(cursor_dt)
    have=current_index_ids()
    want=list(set(new_ids) | have)
    to_upsert=[]; to_delete=[]
    if want:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
            futs={ex.submit(fetch_ticket_min, tid): tid for tid in want}
            for f in as_completed(futs):
                tid=futs[f]
                t=f.result()
                if t and is_resolved(t):
                    st=t.get("status") or ""
                    to_upsert.append((tid, st))
                else:
                    if tid in have:
                        to_delete.append(tid)
    upsert_rows(to_upsert)
    delete_ids(to_delete)
    reopened=list_ids_reopened_since(cursor_dt)
    delete_ids(reopened)
    set_cursor(datetime.now(timezone.utc)-timedelta(minutes=1))

if __name__ == "__main__":
    run()
