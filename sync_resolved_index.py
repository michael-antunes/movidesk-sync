import os, requests, psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN=os.environ["MOVIDESK_TOKEN"]
DSN=os.environ["NEON_DSN"]
BASE="https://api.movidesk.com/public/v1"
SCHEMA="visualizacao_resolvidos"

CONCURRENCY=int(os.getenv("CONCURRENCY","8"))
CONNECT_TIMEOUT=int(os.getenv("CONNECT_TIMEOUT","15"))
READ_TIMEOUT=int(os.getenv("READ_TIMEOUT","120"))
MAX_TRIES=int(os.getenv("MAX_TRIES","6"))

TRIGGER_TITLES=[s.strip() for s in os.getenv("TRIGGER_TITLES","Resolved,Resolvido").split(",") if s.strip()]
CLOSE_TITLES=[s.strip() for s in os.getenv("CLOSE_TITLES","Closed,Fechado").split(",") if s.strip()]
CURSOR_REWIND_MIN=int(os.getenv("CURSOR_REWIND_MIN","5"))
TZ=ZoneInfo("America/Sao_Paulo")

session=requests.Session()
retry_cfg=Retry(total=5,connect=5,read=5,backoff_factor=1,status_forcelist=[429,500,502,503,504],allowed_methods=["GET"])
adapter=HTTPAdapter(max_retries=retry_cfg,pool_connections=CONCURRENCY*2,pool_maxsize=CONCURRENCY*2)
session.mount("https://",adapter); session.mount("http://",adapter)

def api_get(url,params,tries=MAX_TRIES):
    last=None
    for i in range(tries):
        try:
            r=session.get(url,params=params,timeout=(CONNECT_TIMEOUT,READ_TIMEOUT))
            if r.status_code in (429,500,502,503,504):
                _sleep=min(90,2**i); import time; time.sleep(_sleep); continue
            r.raise_for_status(); return r.json()
        except (requests.exceptions.Timeout,requests.exceptions.ConnectionError) as e:
            last=e; import time; time.sleep(min(90,2**i)); continue
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code==404: return None
            last=e; import time; time.sleep(min(60,2**i)); continue
        except ValueError as e:
            last=e; import time; time.sleep(min(30,2**i)); continue
    if last: raise last

def ensure_structure():
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"create schema if not exists {SCHEMA};")
    cur.execute(f"""
        create table if not exists {SCHEMA}.tickets_resolvidos(
          ticket_id integer primary key,
          status text not null,
          last_resolved_at timestamptz not null,
          last_closed_at timestamptz,
          last_update timestamptz,
          responsible_id bigint,
          responsible_name text
        );
    """)
    cur.execute(f"""
        create table if not exists {SCHEMA}.tickets_resolvidos_diario(
          ticket_id integer primary key,
          status text not null,
          last_resolved_at timestamptz not null,
          last_closed_at timestamptz,
          last_update timestamptz,
          responsible_id bigint,
          responsible_name text
        );
    """)
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists last_resolved_at timestamptz not null default now();")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists last_closed_at timestamptz;")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists last_update timestamptz;")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists responsible_id bigint;")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists responsible_name text;")
    cur.execute(f"""
        create table if not exists {SCHEMA}.sync_control(
          name text primary key,
          last_update timestamptz not null
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
    base=row[0] if row else datetime.now(timezone.utc)-timedelta(days=60)
    if base.tzinfo is None: base=base.replace(tzinfo=timezone.utc)
    return base - timedelta(minutes=CURSOR_REWIND_MIN)

def set_cursor(ts):
    if ts.tzinfo is None: ts=ts.replace(tzinfo=timezone.utc)
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"update {SCHEMA}.sync_control set last_update=%s where name='tr_lastresolved'",(ts,))
    cur.close(); conn.close()

def iso(dt): return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def _any_status_eq(field_alias, titles):
    if not titles: return None
    parts=[f"{field_alias}/status eq '{t}'" for t in titles]
    return "("+" or ".join(parts)+")"

def list_ids_resolved_since(cursor_dt):
    ids=set(); top=500; skip=0
    trig=_any_status_eq("s", TRIGGER_TITLES) or "s/status eq 'Resolved'"
    filt=f"statusHistories/any(s: s/changedDate ge {iso(cursor_dt)} and {trig})"
    while True:
        p={"token":TOKEN,"$select":"id","$filter":filt,"$top":top,"$skip":skip}
        batch=api_get(f"{BASE}/tickets/past",p) or []
        if not batch: break
        for t in batch:
            tid=t.get("id")
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch)<top: break
        skip+=top
    return list(ids)

def list_ids_reopened_since(cursor_dt):
    ids=set(); top=500; skip=0
    not_trig=" and ".join([f"s/status ne '{t}'" for t in TRIGGER_TITLES]) if TRIGGER_TITLES else "s/status ne 'Resolved'"
    filt=f"statusHistories/any(s: s/changedDate ge {iso(cursor_dt)} and ({not_trig}))"
    while True:
        p={"token":TOKEN,"$select":"id","$filter":filt,"$top":top,"$skip":skip}
        batch=api_get(f"{BASE}/tickets/past",p) or []
        if not batch: break
        for t in batch:
            tid=t.get("id")
            if tid is not None:
                try: ids.add(int(tid))
                except: pass
        if len(batch)<top: break
        skip+=top
    return list(ids)

def parse_dt(s):
    if not s: return None
    try:
        if isinstance(s,str) and s.endswith("Z"): return datetime.fromisoformat(s.replace("Z","+00:00"))
        return datetime.fromisoformat(s) if isinstance(s,str) else s
    except: return None

def fetch_ticket_min(ticket_id):
    p={'token':TOKEN,'id':ticket_id,'$select':'id,status,baseStatus,lastUpdate,statusHistories,owner'}
    try:
        r=api_get(f"{BASE}/tickets",p)
        if isinstance(r,list): return r[0] if r else None
        return r
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code!=404: raise
    try:
        r=api_get(f"{BASE}/tickets/past",p)
        if isinstance(r,list): return r[0] if r else None
        return r
    except: return None

def is_resolved(t):
    st=(t.get("status") or "").lower()
    bs=(t.get("baseStatus") or "")
    return bs in ("Resolved","Closed") or st in {"resolved","resolvido","closed","fechado"}

def resolved_at(t):
    best=None
    for s in (t.get("statusHistories") or []):
        st=(s.get("status") or "")
        if st in TRIGGER_TITLES:
            dt=parse_dt(s.get("changedDate") or s.get("date") or s.get("changedDateTime"))
            if dt and (best is None or dt>best): best=dt
    return best or parse_dt(t.get("lastUpdate"))

def closed_at(t):
    best=None
    for s in (t.get("statusHistories") or []):
        st=(s.get("status") or "")
        if st in CLOSE_TITLES:
            dt=parse_dt(s.get("changedDate") or s.get("date") or s.get("changedDateTime"))
            if dt and (best is None or dt>best): best=dt
    return best

def extract_responsible(t):
    o=t.get("owner") or {}
    rid=o.get("id")
    rname=o.get("businessName") or o.get("name") or o.get("fullName")
    try: rid=int(rid) if rid is not None else None
    except: rid=None
    return rid,rname

def upsert_rows(table, items):
    if not items: return
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    for tid,st,rat,cat,lupd,rid,rname in items:
        cur.execute(f"""
            insert into {SCHEMA}.{table}
            (ticket_id,status,last_resolved_at,last_closed_at,last_update,responsible_id,responsible_name)
            values (%s,%s,%s,%s,%s,%s,%s)
            on conflict (ticket_id) do update set
                status=excluded.status,
                last_resolved_at=excluded.last_resolved_at,
                last_closed_at=excluded.last_closed_at,
                last_update=excluded.last_update,
                responsible_id=coalesce(excluded.responsible_id,{SCHEMA}.{table}.responsible_id),
                responsible_name=coalesce(excluded.responsible_name,{SCHEMA}.{table}.responsible_name);
        """,(tid,st,rat,cat,lupd,rid,rname))
    cur.close(); conn.close()

def delete_ids(table, ids):
    if not ids: return
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"delete from {SCHEMA}.{table} where ticket_id = any(%s)",(ids,))
    cur.close(); conn.close()

def current_index_ids():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id from {SCHEMA}.tickets_resolvidos")
    data=[r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return set(data)

def in_last_two_days_sp(dt):
    if not dt: return False
    sp=dt.astimezone(TZ)
    start= datetime.now(TZ).replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=1)
    return sp >= start

def prune_diario():
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"""
        delete from {SCHEMA}.tickets_resolvidos_diario
        where (last_resolved_at at time zone 'America/Sao_Paulo')::date < ((now() at time zone 'America/Sao_Paulo')::date - 1)
    """)
    cur.close(); conn.close()

def run():
    ensure_structure()
    cursor_dt=get_cursor()
    new_ids=list_ids_resolved_since(cursor_dt)
    have=current_index_ids()
    want=list(set(new_ids)|have)
    to_upsert=[]; to_delete=[]
    max_seen=cursor_dt
    if want:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
            futs={ex.submit(fetch_ticket_min,tid):tid for tid in want}
            for f in as_completed(futs):
                tid=futs[f]
                try:
                    t=f.result()
                except Exception as e:
                    print("warn: fetch failed for ticket",tid,":",e); continue
                if t and is_resolved(t):
                    st=t.get("status") or ""
                    rat=resolved_at(t) or datetime.now(timezone.utc)
                    if rat and rat.tzinfo is None: rat=rat.replace(tzinfo=timezone.utc)
                    cat=closed_at(t)
                    if cat and cat.tzinfo is None: cat=cat.replace(tzinfo=timezone.utc)
                    lupd=parse_dt(t.get("lastUpdate")) or datetime.now(timezone.utc)
                    rid,rname=extract_responsible(t)
                    to_upsert.append((tid,st,rat,cat,lupd,rid,rname))
                    if rat and rat>max_seen: max_seen=rat
                    if cat and cat>max_seen: max_seen=cat
                else:
                    if tid in have: to_delete.append(tid)
    upsert_rows("tickets_resolvidos", to_upsert)
    delete_ids("tickets_resolvidos", to_delete)
    reopened=list_ids_reopened_since(cursor_dt)
    delete_ids("tickets_resolvidos", reopened)

    recent=[row for row in to_upsert if in_last_two_days_sp(row[2])]
    upsert_rows("tickets_resolvidos_diario", recent)
    delete_ids("tickets_resolvidos_diario", to_delete)
    delete_ids("tickets_resolvidos_diario", reopened)
    prune_diario()

    set_cursor(max_seen)

if __name__=="__main__":
    run()
