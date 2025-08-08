import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
RESOLVED_SET = {'resolved','closed','resolvido','fechado'}
IDX_SCHEMA = 'visualizacao_resolvidos'

def start_of_today_utc():
    n = datetime.now(timezone.utc)
    return datetime(n.year, n.month, n.day, tzinfo=timezone.utc)

def api_get(url, params):
    for i in range(6):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            sleep(min(60, 2**i))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def ensure_structure():
    conn = psycopg2.connect(NEON_DSN); conn.autocommit = True; cur = conn.cursor()
    cur.execute(f"create schema if not exists {IDX_SCHEMA};")
    cur.execute(f"""
        create table if not exists {IDX_SCHEMA}.tickets_resolvidos(
            ticket_id integer primary key,
            protocol text,
            status text not null,
            last_resolved_at timestamp,
            last_update timestamp,
            monitor_until timestamp,
            imported_at timestamp default now()
        );
    """)
    cur.execute(f"""
        create table if not exists {IDX_SCHEMA}.sync_control(
            name text primary key,
            last_update timestamp not null
        );
    """)
    cur.execute(f"insert into {IDX_SCHEMA}.sync_control(name,last_update) values('tr_lastresolved', %s) on conflict (name) do nothing;", (start_of_today_utc(),))
    cur.close(); conn.close()

def get_cursor():
    conn = psycopg2.connect(NEON_DSN); conn.autocommit = True; cur = conn.cursor()
    cur.execute(f"select last_update from {IDX_SCHEMA}.sync_control where name='tr_lastresolved'")
    row = cur.fetchone(); cur.close(); conn.close()
    dt = row[0] if row else start_of_today_utc()
    if isinstance(dt,str):
        try: dt = datetime.fromisoformat(dt.replace('Z','+00:00'))
        except Exception: dt = start_of_today_utc()
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    st = start_of_today_utc()
    return dt if dt > st else st

def set_cursor(ts):
    if ts is None: return
    if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
    conn = psycopg2.connect(NEON_DSN); conn.autocommit = True; cur = conn.cursor()
    cur.execute(f"update {IDX_SCHEMA}.sync_control set last_update=%s where name='tr_lastresolved'", (ts,))
    cur.close(); conn.close()

def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def list_ids_resolved_since(cursor_dt):
    st = start_of_today_utc()
    cursor_dt = cursor_dt if cursor_dt > st else st
    ids=set(); max_res=cursor_dt; top=500; skip=0
    filt = "statusHistories/any(s: s/changedDate ge "+iso(cursor_dt)+" and ("+ " or ".join([f"tolower(s/status) eq '{s}'" for s in RESOLVED_SET]) +"))"
    while True:
        params={'token':MOVIDESK_TOKEN,'$select':'id','$filter':filt,'$top':top,'$skip':skip}
        batch=api_get('https://api.movidesk.com/public/v1/tickets/past',params)
        if not batch: break
        for t in batch:
            tid=t.get('id')
            if tid is not None: ids.add(int(tid))
        if len(batch)<top: break
        skip+=len(batch)
    return list(ids), max_res

def fetch_ticket_detail(ticket_id):
    sel='id,protocol,status,baseStatus,lastUpdate'
    exp='statusHistories($select=status,changedDate)'
    p={'token':MOVIDESK_TOKEN,'id':ticket_id,'$select':sel,'$expand':exp}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    return api_get('https://api.movidesk.com/public/v1/tickets/past', p)

def fetch_ticket_min(ticket_id):
    sel='id,status,baseStatus,lastUpdate'
    p={'token':MOVIDESK_TOKEN,'id':ticket_id,'$select':sel}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    return api_get('https://api.movidesk.com/public/v1/tickets/past', p)

def is_resolved(t):
    bs=(t or {}).get('baseStatus') or ''
    st=(t or {}).get('status') or ''
    if bs in ('Resolved','Closed'): return True
    if (st or '').lower() in RESOLVED_SET: return True
    return False

def latest_resolved_changed_date(ticket):
    ts=None
    for h in ticket.get('statusHistories',[]) or []:
        s=(h.get('status') or '').lower()
        if s in RESOLVED_SET:
            cd=h.get('changedDate')
            if cd:
                try:
                    dt=datetime.fromisoformat(cd.replace('Z','+00:00'))
                    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
                    if ts is None or dt>ts: ts=dt
                except Exception: pass
    return ts

def upsert_index(conn, tid, protocol, status, last_resolved_at, last_update, monitor_until):
    cur=conn.cursor()
    cur.execute(f"""
        insert into {IDX_SCHEMA}.tickets_resolvidos(ticket_id, protocol, status, last_resolved_at, last_update, monitor_until, imported_at)
        values (%s,%s,%s,%s,%s,%s, now())
        on conflict (ticket_id) do update set
          protocol=excluded.protocol,
          status=excluded.status,
          last_resolved_at=excluded.last_resolved_at,
          last_update=excluded.last_update,
          monitor_until=excluded.monitor_until,
          imported_at=now();
    """,(tid,protocol,status,last_resolved_at,last_update,monitor_until))
    cur.close()

def remove_from_index(conn, tid):
    cur=conn.cursor()
    cur.execute(f"delete from {IDX_SCHEMA}.tickets_resolvidos where ticket_id=%s",(tid,))
    cur.close()

def load_watchlist():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, coalesce(last_update, timestamp '1970-01-01') from {IDX_SCHEMA}.tickets_resolvidos where monitor_until >= now()")
    data=cur.fetchall(); cur.close(); conn.close()
    return {tid:lu for tid,lu in data}

def run():
    ensure_structure()
    cursor = get_cursor()
    ids, _ = list_ids_resolved_since(cursor)
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True
    max_resolved_seen=cursor
    for tid in ids:
        try:
            t=fetch_ticket_detail(tid)
        except Exception as e:
            print("skip", tid, str(e)); continue
        if not is_resolved(t):
            remove_from_index(conn, tid); continue
        lr=latest_resolved_changed_date(t)
        lu=t.get('lastUpdate')
        if isinstance(lu,str):
            try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
            except Exception: lu=None
        mu=(lr or datetime.now(timezone.utc))+timedelta(days=30)
        upsert_index(conn, t.get('id'), t.get('protocol'), t.get('status'), lr, lu, mu)
        if lr and (max_resolved_seen is None or lr>max_resolved_seen): max_resolved_seen=lr
    watch=load_watchlist()
    if watch:
        def check(tid):
            try:
                t=fetch_ticket_min(tid)
                return tid,t
            except Exception as e:
                return tid,{"error":str(e)}
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs=[ex.submit(check, tid) for tid in watch.keys()]
            for f in as_completed(futs):
                tid,t=f.result()
                if isinstance(t,dict) and t.get("error"):
                    print("skip", tid, t["error"]); continue
                if not is_resolved(t):
                    remove_from_index(conn, tid); continue
                lu=t.get('lastUpdate')
                if isinstance(lu,str):
                    try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                    except Exception: lu=None
                if lu and lu>watch.get(tid):
                    try:
                        td=fetch_ticket_detail(tid)
                    except Exception as e:
                        print("skip", tid, str(e)); continue
                    lr=latest_resolved_changed_date(td)
                    mu=(lr or datetime.now(timezone.utc))+timedelta(days=30)
                    upsert_index(conn, tid, td.get('protocol'), td.get('status'), lr, lu, mu)
    conn.close()
    if max_resolved_seen: set_cursor(max_resolved_seen - timedelta(minutes=1))

if __name__ == '__main__':
    run()
