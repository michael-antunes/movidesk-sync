import os
import json
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
DET_SCHEMA = 'visualizacao_resolucao'

def start_of_today_utc():
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

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
    cur.execute(f"create schema if not exists {DET_SCHEMA};")
    cur.execute(f"create schema if not exists {IDX_SCHEMA};")
    cur.execute(f"""
        create table if not exists {DET_SCHEMA}.resolucao_por_status(
            ticket_id integer not null,
            protocol text,
            status text not null,
            justificativa text not null,
            seconds_uti integer,
            permanency_time_fulltime_seconds double precision,
            changed_by jsonb,
            changed_date timestamp,
            agent_name text default '',
            team_name text default '',
            imported_at timestamp default now(),
            primary key (ticket_id, status, justificativa)
        );
    """)
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
    cur.execute(f"create table if not exists {IDX_SCHEMA}.sync_control(name text primary key, last_update timestamp not null);")
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
    sel='id,protocol,status,baseStatus,ownerTeam,lastUpdate'
    exp='statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))'
    p_id={'token':MOVIDESK_TOKEN,'id':ticket_id,'$select':sel,'$expand':exp}
    p_f1={'token':MOVIDESK_TOKEN,'$select':sel,'$expand':exp,'$filter':f'id eq {int(ticket_id)}','$top':1}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p_id)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets/past', p_id)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    batch = api_get('https://api.movidesk.com/public/v1/tickets', p_f1)
    if isinstance(batch, list) and batch: return batch[0]
    batch = api_get('https://api.movidesk.com/public/v1/tickets/past', p_f1)
    if isinstance(batch, list) and batch: return batch[0]
    return {}

def fetch_ticket_min(ticket_id):
    sel='id,status,baseStatus,lastUpdate'
    p_id={'token':MOVIDESK_TOKEN,'id':ticket_id,'$select':sel}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p_id)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    return api_get('https://api.movidesk.com/public/v1/tickets/past', p_id)

def is_resolved(ticket):
    bs=(ticket or {}).get('baseStatus') or ''
    st=(ticket or {}).get('status') or ''
    if bs in ('Resolved','Closed'): return True
    if (st or '').lower() in RESOLVED_SET: return True
    return False

def owner_team_name(ticket):
    ot=ticket.get('ownerTeam')
    if isinstance(ot,dict): return ot.get('businessName') or ''
    if isinstance(ot,str): return ot
    return ''

def teams_to_name(teams):
    names=[]
    if isinstance(teams,list):
        for t in teams:
            if isinstance(t,dict) and t.get('businessName'): names.append(t['businessName'])
            elif isinstance(t,str): names.append(t)
    return ', '.join(sorted(set([n for n in names if n])))

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

def extract_rows(ticket):
    tid=ticket.get('id'); protocol=ticket.get('protocol'); owner_team=owner_team_name(ticket); rows=[]
    for h in ticket.get('statusHistories',[]) or []:
        status=h.get('status') or ''
        justification=h.get('justification') or ''
        sec_work=h.get('permanencyTimeWorkingTime') or 0
        sec_full=h.get('permanencyTimeFullTime') or 0
        changed_by=h.get('changedBy') or {}
        agent=changed_by.get('businessName') if isinstance(changed_by,dict) else ''
        team=teams_to_name(changed_by.get('teams')) if isinstance(changed_by,dict) else ''
        if not team:
            cbt=h.get('changedByTeam')
            if isinstance(cbt,dict) and cbt.get('businessName'): team=cbt['businessName']
            elif isinstance(cbt,str): team=cbt
        if not team: team=owner_team
        changed_date=h.get('changedDate')
        rows.append((tid,protocol,status,justification,int(sec_work),float(sec_full),json.dumps(changed_by,ensure_ascii=False),changed_date,agent,team))
    return rows

def dedupe_by_pk_keep_latest(rows):
    best={}
    for r in rows:
        key=(r[0], r[2], r[3])
        prev=best.get(key)
        cur_cd=r[7] or ''
        if prev is None:
            best[key]=r
        else:
            prev_cd=prev[7] or ''
            if cur_cd >= prev_cd:
                best[key]=r
    return list(best.values())

def delete_ticket_rows(conn, ticket_id):
    cur=conn.cursor()
    cur.execute(f"delete from {DET_SCHEMA}.resolucao_por_status where ticket_id=%s",(ticket_id,))
    cur.close()

def upsert_detail(conn, rows):
    if not rows: return
    cur=conn.cursor()
    sql=f"""
        insert into {DET_SCHEMA}.resolucao_por_status
        (ticket_id, protocol, status, justificativa, seconds_uti, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
        values %s
        on conflict (ticket_id, status, justificativa) do update set
            protocol = excluded.protocol,
            seconds_uti = excluded.seconds_uti,
            permanency_time_fulltime_seconds = excluded.permanency_time_fulltime_seconds,
            changed_by = excluded.changed_by,
            changed_date = excluded.changed_date,
            agent_name = excluded.agent_name,
            team_name = excluded.team_name,
            imported_at = now();
    """
    execute_values(cur, sql, rows)
    cur.close()

def upsert_index(conn, ticket_id, protocol, status, last_resolved_at, last_update, monitor_until):
    cur=conn.cursor()
    cur.execute(f"""
        insert into {IDX_SCHEMA}.tickets_resolvidos(ticket_id, protocol, status, last_resolved_at, last_update, monitor_until, imported_at)
        values (%s,%s,%s,%s,%s,%s, now())
        on conflict (ticket_id) do update set
          protocol = excluded.protocol,
          status = excluded.status,
          last_resolved_at = excluded.last_resolved_at,
          last_update = excluded.last_update,
          monitor_until = excluded.monitor_until,
          imported_at = now();
    """,(ticket_id, protocol, status, last_resolved_at, last_update, monitor_until))
    cur.close()

def remove_from_index_and_detail(conn, ticket_id):
    cur=conn.cursor()
    cur.execute(f"delete from {DET_SCHEMA}.resolucao_por_status where ticket_id=%s",(ticket_id,))
    cur.execute(f"delete from {IDX_SCHEMA}.tickets_resolvidos where ticket_id=%s",(ticket_id,))
    cur.close()

def load_watchlist():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, coalesce(last_update, timestamp '1970-01-01') from {IDX_SCHEMA}.tickets_resolvidos where monitor_until >= now()")
    data=cur.fetchall(); cur.close(); conn.close()
    return {tid:lu for tid,lu in data}

def run():
    ensure_structure()
    cr=get_cursor()
    ids_r,_=list_ids_resolved_since(cr)
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True
    max_resolved_seen=cr
    for tid in ids_r:
        try:
            ticket=fetch_ticket_detail(tid)
        except Exception as e:
            print("skip", tid, str(e)); continue
        if not is_resolved(ticket):
            remove_from_index_and_detail(conn, tid); continue
        lr=latest_resolved_changed_date(ticket)
        lu=ticket.get('lastUpdate')
        if isinstance(lu,str):
            try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
            except Exception: lu=None
        monitor_until=(lr or datetime.now(timezone.utc)) + timedelta(days=30)
        cur=conn.cursor()
        cur.execute(f"select last_resolved_at, last_update from {IDX_SCHEMA}.tickets_resolvidos where ticket_id=%s",(tid,))
        row=cur.fetchone(); cur.close()
        upsert_index(conn, ticket.get('id'), ticket.get('protocol'), ticket.get('status'), lr, lu, monitor_until)
        if not row or row[0]!=lr or row[1]!=lu:
            delete_ticket_rows(conn, tid)
            rows=dedupe_by_pk_keep_latest(extract_rows(ticket))
            upsert_detail(conn, rows)
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
            futures=[ex.submit(check, tid) for tid in watch.keys()]
            for fut in as_completed(futures):
                tid,t=fut.result()
                if isinstance(t,dict) and t.get("error"):
                    print("skip", tid, t["error"]); continue
                if not is_resolved(t):
                    remove_from_index_and_detail(conn, tid); continue
                lu=t.get('lastUpdate')
                if isinstance(lu,str):
                    try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                    except Exception: lu=None
                if lu and lu>watch.get(tid):
                    ticket=fetch_ticket_detail(tid)
                    if is_resolved(ticket):
                        lr=latest_resolved_changed_date(ticket)
                        monitor_until=(lr or datetime.now(timezone.utc))+timedelta(days=30)
                        upsert_index(conn, ticket.get('id'), ticket.get('protocol'), ticket.get('status'), lr, lu, monitor_until)
                        delete_ticket_rows(conn, tid)
                        rows=dedupe_by_pk_keep_latest(extract_rows(ticket))
                        upsert_detail(conn, rows)
                    else:
                        remove_from_index_and_detail(conn, tid)
    conn.close()
    if max_resolved_seen: set_cursor(max_resolved_seen - timedelta(minutes=1))

if __name__ == '__main__':
    run()
