import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
WINDOW_DAYS = int(os.getenv('WINDOW_DAYS', '60'))
RESOLVED_SET = {'resolved','closed','resolvido','fechado'}
SCHEMA = 'visualizacao_resolvidos'
TABLE = f'{SCHEMA}.tickets_resolvidos'

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
    cur.execute(f"create schema if not exists {SCHEMA};")
    cur.execute(f"""
        create table if not exists {TABLE}(
            ticket_id integer primary key,
            protocol text,
            status text not null,
            last_resolved_at timestamp,
            last_update timestamp,
            imported_at timestamp default now()
        );
    """)
    cur.close(); conn.close()

def wstart():
    return datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)

def iso(dt): 
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def list_ids_resolved_in_window(start_dt):
    ids=set(); top=500; skip=0
    conds = " or ".join([f"tolower(s/status) eq '{s}'" for s in RESOLVED_SET])
    filt = f"statusHistories/any(s: s/changedDate ge {iso(start_dt)} and ({conds}))"
    while True:
        params={'token':MOVIDESK_TOKEN,'$select':'id','$filter':filt,'$top':top,'$skip':skip}
        batch=api_get('https://api.movidesk.com/public/v1/tickets/past',params)
        if not batch: break
        for t in batch:
            tid=t.get('id')
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch)<top: break
        skip+=len(batch)
    return list(ids)

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
    sel='id,status,baseStatus,lastUpdate,protocol'
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

def upsert_rows(rows):
    if not rows: return
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    sql=f"""
        insert into {TABLE}(ticket_id, protocol, status, last_resolved_at, last_update, imported_at)
        values %s
        on conflict (ticket_id) do update set
          protocol=excluded.protocol,
          status=excluded.status,
          last_resolved_at=excluded.last_resolved_at,
          last_update=excluded.last_update,
          imported_at=now();
    """
    execute_values(cur, sql, rows); cur.close(); conn.close()

def delete_ids(ids):
    if not ids: return
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    execute_values(cur, f"delete from {TABLE} where ticket_id in %s", [tuple(ids)])
    cur.close(); conn.close()

def delete_older_than(start_dt):
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"delete from {TABLE} where last_resolved_at < %s", (start_dt,))
    cur.close(); conn.close()

def load_current_ids():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, coalesce(last_update, timestamp '1970-01-01') from {TABLE}")
    data=cur.fetchall(); cur.close(); conn.close()
    return {tid:lu for tid,lu in data}

def run():
    ensure_structure()
    start = wstart()
    ids = list_ids_resolved_in_window(start)
    rows=[]
    for tid in ids:
        try:
            t=fetch_ticket_detail(tid)
        except Exception:
            continue
        if not is_resolved(t):
            continue
        lr=latest_resolved_changed_date(t)
        lu=t.get('lastUpdate')
        if isinstance(lu,str):
            try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
            except Exception: lu=None
        rows.append((t.get('id'), t.get('protocol'), t.get('status'), lr, lu, datetime.now(timezone.utc)))
    upsert_rows(rows)
    current=load_current_ids()
    to_check=list(current.keys())
    reopened=[]
    changed=[]
    def chk(tid):
        try: return tid, fetch_ticket_min(tid)
        except Exception: return tid, {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs=[ex.submit(chk, tid) for tid in to_check]
        for f in as_completed(futs):
            tid,t=f.result()
            if not is_resolved(t):
                reopened.append(tid); continue
            lu=t.get('lastUpdate')
            if isinstance(lu,str):
                try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                except Exception: lu=None
            if lu and lu>current.get(tid):
                try:
                    td=fetch_ticket_detail(tid)
                except Exception:
                    continue
                lr=latest_resolved_changed_date(td)
                changed.append((td.get('id'), td.get('protocol'), td.get('status'), lr, lu, datetime.now(timezone.utc)))
    upsert_rows(changed)
    delete_ids(reopened)
    delete_older_than(start)

if __name__ == '__main__':
    run()
