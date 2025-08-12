import os, time, requests, psycopg2
from datetime import datetime, timezone, timedelta

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]
BASE  = "https://api.movidesk.com/public/v1"
SCHEMA = "visualizacao_resolvidos"
RESOLVED = {"resolved","closed","resolvido","fechado"}
GROUP = 24

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
          status    text not null,
          imported_at timestamp default now()
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
    dt=row[0] if row else datetime.now(timezone.utc)-timedelta(days=60)
    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
    return dt

def set_cursor(ts):
    if ts.tzinfo is None: ts=ts.replace(tzinfo=timezone.utc)
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"update {SCHEMA}.sync_control set last_update=%s where name='tr_lastresolved'", (ts,))
    cur.close(); conn.close()

def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def list_ids_resolved_since(cursor_dt):
    ids=set(); top=500; skip=0
    filt = "statusHistories/any(s: s/changedDate ge "+iso(cursor_dt)+" and ("+ " or ".join([f"tolower(s/status) eq '{s}'" for s in RESOLVED]) +"))"
    while True:
        p={"token":TOKEN,"$select":"id","$filter":filt,"$top":top,"$skip":skip}
        batch = api_get(f"{BASE}/tickets/past", p) or []
        if not batch: break
        for t in batch:
            if "id" in t: ids.add(int(t["id"]))
        if len(batch) < top: break
        skip += top
    return list(ids)

def fetch_min_batch(id_batch):
    parts=[f"id eq {i}" for i in id_batch]
    filt="(" + " or ".join(parts) + ")"
    p={"token":TOKEN,"$select":"id,status,baseStatus,lastUpdate","$filter":filt,"$top":len(id_batch),"$skip":0}
    try:
        data = api_get(f"{BASE}/tickets", p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
        data = []
    return data or []

def is_resolved(t):
    st=(t.get("status") or "").lower()
    bs=(t.get("baseStatus") or "")
    if bs in ("Resolved","Closed"): return True
    return st in RESOLVED

def upsert_resolved(conn, items):
    if not items: return
    cur=conn.cursor()
    for t in items:
        cur.execute(f"""
            insert into {SCHEMA}.tickets_resolvidos(ticket_id, status, imported_at)
            values (%s,%s,now())
            on conflict (ticket_id) do update set status=excluded.status, imported_at=now();
        """,(int(t["id"]), t.get("status") or ""))
    cur.close()

def delete_ids(conn, ids):
    if not ids: return
    cur=conn.cursor()
    cur.execute(f"delete from {SCHEMA}.tickets_resolvidos where ticket_id = any(%s)", (ids,))
    cur.close()

def current_index_ids():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id from {SCHEMA}.tickets_resolvidos")
    data=[r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return set(data)

def run():
    ensure_structure()
    cursor_dt=get_cursor()
    ids_new=list_ids_resolved_since(cursor_dt)
    have=current_index_ids()
    want=set(ids_new) | have
    if not want:
        set_cursor(datetime.now(timezone.utc)-timedelta(minutes=1))
        return
    conn=psycopg2.connect(DSN); conn.autocommit=True
    to_del=[]
    buf=list(want)
    for i in range(0,len(buf),GROUP):
        batch=buf[i:i+GROUP]
        minis=fetch_min_batch(batch)
        by_id={int(t["id"]):t for t in minis}
        keep=[]
        for tid in batch:
            t=by_id.get(int(tid))
            if t and is_resolved(t):
                keep.append(t)
            else:
                if tid in have:
                    to_del.append(tid)
        upsert_resolved(conn, keep)
    delete_ids(conn, to_del)
    conn.close()
    set_cursor(datetime.now(timezone.utc)-timelta(minutes=1))

if __name__ == "__main__":
    run()
