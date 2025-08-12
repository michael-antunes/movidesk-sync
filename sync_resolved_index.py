import os, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "60"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
PAGE_SIZE = 500
SCHEMA = "visualizacao_resolvidos"
TABLE = f"{SCHEMA}.tickets_resolvidos"
CTRL  = f"{SCHEMA}.sync_control"
CURSOR_NAME = "tr_lastresolved"
RESOLVED_SET = {"resolved","closed","resolvido","fechado"}

def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def api_get(url, params):
    for i in range(6):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r.json()
    r.raise_for_status()

def ensure_structure():
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
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
    cur.execute(f"""
        create table if not exists {CTRL}(
            name text primary key,
            last_update timestamp not null
        );
    """)
    cur.execute(f"insert into {CTRL}(name,last_update) values(%s, now() - interval '60 days') on conflict (name) do nothing;", (CURSOR_NAME,))
    cur.close(); conn.close()

def get_cursor():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select last_update from {CTRL} where name=%s", (CURSOR_NAME,))
    row=cur.fetchone(); cur.close(); conn.close()
    c = row[0] if row else datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    if isinstance(c,str):
        try: c=datetime.fromisoformat(c.replace('Z','+00:00'))
        except: c=datetime.now(timezone.utc)-timedelta(days=WINDOW_DAYS)
    if c.tzinfo is None: c=c.replace(tzinfo=timezone.utc)
    floor = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    return max(c, floor)

def set_cursor(ts):
    if ts.tzinfo is None: ts=ts.replace(tzinfo=timezone.utc)
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"update {CTRL} set last_update=%s where name=%s", (ts, CURSOR_NAME))
    cur.close(); conn.close()

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
        bs=(h.get('baseStatus') or '').lower()
        if s in RESOLVED_SET or bs in ('resolved','closed'):
            cd=h.get('changedDate')
            if cd:
                try:
                    dt=datetime.fromisoformat(cd.replace('Z','+00:00'))
                    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
                    if ts is None or dt>ts: ts=dt
                except Exception: pass
    return ts

def list_ids_resolved_since(start_dt):
    ids=set(); skip=0
    conds = " or ".join([f"tolower(s/status) eq '{s}'" for s in RESOLVED_SET])
    filt  = f"statusHistories/any(s: s/changedDate ge {iso(start_dt)} and ({conds}))"
    while True:
        p={'token':TOKEN,'$select':'id','$filter':filt,'$top':PAGE_SIZE,'$skip':skip}
        batch=api_get('https://api.movidesk.com/public/v1/tickets/past', p)
        if not batch: break
        for t in batch:
            tid=t.get('id')
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch)<PAGE_SIZE: break
        skip+=PAGE_SIZE
    return list(ids)

def list_ids_reopened_since(start_dt):
    ids=set(); skip=0
    conds = " and ".join([f"tolower(s/status) ne '{s}'" for s in RESOLVED_SET])
    filt  = f"statusHistories/any(s: s/changedDate ge {iso(start_dt)} and {conds})"
    while True:
        p={'token':TOKEN,'$select':'id','$filter':filt,'$top':PAGE_SIZE,'$skip':skip}
        batch=api_get('https://api.movidesk.com/public/v1/tickets/past', p)
        if not batch: break
        for t in batch:
            tid=t.get('id')
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch)<PAGE_SIZE: break
        skip+=PAGE_SIZE
    return list(ids)

def fetch_ticket_detail(ticket_id):
    sel='id,protocol,status,baseStatus,lastUpdate'
    exp='statusHistories($select=status,changedDate,baseStatus)'
    p={'token':TOKEN,'id':ticket_id,'$select':sel,'$expand':exp}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    return api_get('https://api.movidesk.com/public/v1/tickets/past', p)

def upsert_rows(rows):
    if not rows: return
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
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
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    execute_values(cur, f"delete from {TABLE} where ticket_id in %s", [tuple(ids)])
    cur.close(); conn.close()

def load_current_map():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, coalesce(last_update, timestamp '1970-01-01') from {TABLE}")
    m={tid:lu for tid,lu in cur.fetchall()}
    cur.close(); conn.close()
    return m

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_changed_min_since(cursor_dt, ids):
    if not ids: return []
    cond_ids = " or ".join([f"id eq {i}" for i in ids])
    filt = f"({cond_ids}) and lastUpdate ge {iso(cursor_dt)}"
    p={'token':TOKEN,'$select':'id,status,baseStatus,lastUpdate,protocol','$filter':filt,'$top':PAGE_SIZE,'$skip':0}
    r=api_get('https://api.movidesk.com/public/v1/tickets', p)
    if not isinstance(r, list): r=[r] if r else []
    return r

def run():
    ensure_structure()
    cursor = get_cursor()
    now_ts = datetime.now(timezone.utc)

    new_ids = list_ids_resolved_since(cursor)
    rows=[]
    def get_detail(tid):
        try: return tid, fetch_ticket_detail(tid)
        except Exception as e: return tid, {"error":str(e)}
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs=[ex.submit(get_detail, tid) for tid in new_ids]
        for f in as_completed(futs):
            tid,t=f.result()
            if isinstance(t,dict) and t.get("error"): continue
            if not is_resolved(t): continue
            lr=latest_resolved_changed_date(t)
            lu=t.get('lastUpdate')
            if isinstance(lu,str):
                try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                except Exception: lu=None
            rows.append((t.get('id'), t.get('protocol'), t.get('status'), lr, lu, now_ts))
    upsert_rows(rows)

    reopened = list_ids_reopened_since(cursor)
    if reopened: delete_ids(reopened)

    current_map = load_current_map()
    changed_rows=[]
    id_list = list(current_map.keys())
    for group in chunks(id_list, 40):
        minis = fetch_changed_min_since(cursor, group)
        if not minis: continue
        for m in minis:
            lu=m.get('lastUpdate')
            if isinstance(lu,str):
                try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                except Exception: lu=None
            if not lu: continue
            if lu <= current_map.get(m.get('id')): continue
            tid=m.get('id')
            try:
                t=fetch_ticket_detail(tid)
            except Exception:
                continue
            if not is_resolved(t):
                reopened.append(tid); continue
            lr=latest_resolved_changed_date(t)
            changed_rows.append((t.get('id'), t.get('protocol'), t.get('status'), lr, lu, now_ts))
    upsert_rows(changed_rows)
    if reopened: delete_ids(list(set(reopened)))

    expire_before = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"delete from {TABLE} where coalesce(last_resolved_at, imported_at) < %s", (expire_before,))
    cur.close(); conn.close()

    set_cursor(now_ts - timedelta(minutes=1))

if __name__ == "__main__":
    run()
