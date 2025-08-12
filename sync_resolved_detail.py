import os, json, time, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]
NEON_DSN = os.environ["NEON_DSN"]
IDX_SCHEMA = "visualizacao_resolvidos"
DET_SCHEMA = "visualizacao_resolucao"
RESOLVED_SET = {"resolved","closed","resolvido","fechado"}
GENERIC_TEAMS = {"administradores","agente administrador","administrators","agent administrator","default","geral","todos","all","users","usuários","colaboradores"}
TEAM_PRIORITY = ["telefone","chat","n1","n2","cs","suporte","service desk","desenvolvimento","squad","projeto"]
CONCURRENCY = int(os.getenv("CONCURRENCY","8"))

def api_get(url, params):
    for i in range(6):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r.json()
    r.raise_for_status()

def ensure_structure():
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"create schema if not exists {DET_SCHEMA};")
    cur.execute(f"""
        create table if not exists {DET_SCHEMA}.resolucao_por_status(
            ticket_id integer not null,
            status text not null,
            justificativa text not null,
            seconds_uti integer,
            permanency_time_fulltime_seconds double precision,
            changed_by jsonb,
            changed_date timestamp,
            agent_name text default '',
            team_name text default '',
            imported_at timestamp default now(),
            primary key (ticket_id, status, justificativa, changed_date)
        );
    """)
    cur.execute(f"""
        create table if not exists {IDX_SCHEMA}.detail_control(
            ticket_id integer primary key,
            last_resolved_at timestamp,
            last_update timestamp,
            synced_at timestamp
        );
    """)
    cur.close(); conn.close()

def fetch_ticket_detail(ticket_id):
    sel="id,protocol,status,baseStatus,ownerTeam,lastUpdate"
    exp="statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))"
    p={"token":MOVIDESK_TOKEN,"id":ticket_id,"$select":sel,"$expand":exp}
    try:
        r=api_get("https://api.movidesk.com/public/v1/tickets", p)
        if isinstance(r,list): return r[0] if r else None
        return r
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code!=404: raise
    r=api_get("https://api.movidesk.com/public/v1/tickets/past", p)
    if isinstance(r,list): return r[0] if r else None
    return r

def fetch_ticket_min(ticket_id):
    p={"token":MOVIDESK_TOKEN,"id":ticket_id,"$select":"id,status,baseStatus,lastUpdate"}
    try:
        r=api_get("https://api.movidesk.com/public/v1/tickets", p)
        if isinstance(r,list): return r[0] if r else None
        return r
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code!=404: raise
    r=api_get("https://api.movidesk.com/public/v1/tickets/past", p)
    if isinstance(r,list): return r[0] if r else None
    return r

def is_resolved(t):
    bs=(t or {}).get("baseStatus") or ""
    st=((t or {}).get("status") or "").lower()
    if bs in ("Resolved","Closed"): return True
    return st in RESOLVED_SET

def owner_team_name(ticket):
    ot=ticket.get("ownerTeam")
    if isinstance(ot,dict): return ot.get("businessName") or ""
    if isinstance(ot,str): return ot
    return ""

def pick_team(changed_by, owner_team, changed_by_team):
    name=""
    if isinstance(changed_by_team, dict):
        name=(changed_by_team.get("businessName") or "").strip()
    elif isinstance(changed_by_team, str):
        name=changed_by_team.strip()
    if name: return name
    names=[]
    if isinstance(changed_by, dict):
        teams=changed_by.get("teams")
        if isinstance(teams, list):
            for t in teams:
                n=(t.get("businessName") if isinstance(t,dict) else (t if isinstance(t,str) else "")) or ""
                n=n.strip()
                if n and n.lower() not in GENERIC_TEAMS and not n.lower().startswith("admin"):
                    names.append(n)
    names=list(dict.fromkeys(names))
    if not names and isinstance(changed_by, dict):
        teams=changed_by.get("teams")
        if isinstance(teams, list):
            for t in teams:
                n=(t.get("businessName") if isinstance(t,dict) else (t if isinstance(t,str) else "")) or ""
                n=n.strip()
                if n: names.append(n)
        names=list(dict.fromkeys(names))
    if names:
        lowered=[n.lower() for n in names]
        for key in TEAM_PRIORITY:
            for i,low in enumerate(lowered):
                if key in low: return names[i]
        return names[0]
    return owner_team or ""

def latest_resolved_changed_date(ticket):
    ts=None
    for h in ticket.get("statusHistories",[]) or []:
        s=(h.get("status") or "").lower()
        if s in RESOLVED_SET:
            cd=h.get("changedDate")
            if cd:
                try:
                    dt=datetime.fromisoformat(cd.replace("Z","+00:00"))
                    if ts is None or dt>ts: ts=dt
                except: pass
    return ts

def extract_rows(ticket):
    tid=ticket.get("id")
    owner=owner_team_name(ticket)
    rows=[]
    for h in ticket.get("statusHistories",[]) or []:
        status=h.get("status") or ""
        justification=h.get("justification") or ""
        sec_work=h.get("permanencyTimeWorkingTime") or 0
        sec_full=h.get("permanencyTimeFullTime") or 0
        changed_by=h.get("changedBy") or {}
        agent=changed_by.get("businessName") if isinstance(changed_by,dict) else ""
        team=pick_team(changed_by, owner, h.get("changedByTeam"))
        changed_date=h.get("changedDate")
        rows.append((tid,status,justification,int(sec_work),float(sec_full),json.dumps(changed_by,ensure_ascii=False),changed_date,agent,team))
    return rows

def dedupe_rows(rows):
    seen=set(); out=[]
    for r in rows:
        key=(r[0],r[1],r[2],r[6])
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def delete_ticket_rows(conn, tid):
    cur=conn.cursor(); cur.execute(f"delete from {DET_SCHEMA}.resolucao_por_status where ticket_id=%s",(tid,)); cur.close()

def upsert_detail(conn, rows):
    if not rows: return
    cur=conn.cursor()
    sql=f"""
        insert into {DET_SCHEMA}.resolucao_por_status
        (ticket_id, status, justificativa, seconds_uti, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
        values %s
        on conflict (ticket_id, status, justificativa, changed_date) do update set
            seconds_uti=excluded.seconds_uti,
            permanency_time_fulltime_seconds=excluded.permanency_time_fulltime_seconds,
            changed_by=excluded.changed_by,
            agent_name=excluded.agent_name,
            team_name=excluded.team_name,
            imported_at=now();
    """
    execute_values(cur, sql, rows); cur.close()

def upsert_control(conn, tid, lr, lu):
    cur=conn.cursor()
    cur.execute(f"""
        insert into {IDX_SCHEMA}.detail_control(ticket_id,last_resolved_at,last_update,synced_at)
        values (%s,%s,%s,now())
        on conflict (ticket_id) do update set
          last_resolved_at=excluded.last_resolved_at,
          last_update=excluded.last_update,
          synced_at=now();
    """,(tid,lr,lu))
    cur.close()

def current_index_ids():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id from {IDX_SCHEMA}.tickets_resolvidos")
    ids=[r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return set(ids)

def load_watchlist():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, coalesce(last_update, timestamp '1970-01-01') from {IDX_SCHEMA}.detail_control")
    data=cur.fetchall(); cur.close(); conn.close()
    return {tid:lu for tid,lu in data}

def run():
    ensure_structure()
    resolved_ids=current_index_ids()
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True
    cur=conn.cursor()
    cur.execute(f"""
        select tr.ticket_id
        from {IDX_SCHEMA}.tickets_resolvidos tr
        left join {IDX_SCHEMA}.detail_control d on d.ticket_id=tr.ticket_id
        where d.ticket_id is null
    """)
    missing=[r[0] for r in cur.fetchall()]
    cur.close()
    for tid in missing:
        try:
            t=fetch_ticket_detail(tid)
            if not t or not is_resolved(t): continue
            rows=dedupe_rows(extract_rows(t))
            delete_ticket_rows(conn, tid)
            upsert_detail(conn, rows)
            lr=latest_resolved_changed_date(t)
            lu=t.get("lastUpdate")
            if isinstance(lu,str):
                try: lu=datetime.fromisoformat(lu.replace("Z","+00:00"))
                except: lu=None
            upsert_control(conn, tid, lr, lu)
        except Exception: pass
    watch=load_watchlist()
    watch={tid:watch[tid] for tid in watch.keys() if tid in resolved_ids}
    if watch:
        def check(tid):
            try:
                t=fetch_ticket_min(tid)
                return tid,t
            except Exception as e:
                return tid,{"error":str(e)}
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
            futs=[ex.submit(check, tid) for tid in watch.keys()]
            for f in as_completed(futs):
                tid,t=f.result()
                if isinstance(t,dict) and t.get("error"): continue
                if not is_resolved(t): 
                    delete_ticket_rows(conn, tid)
                    cur=conn.cursor(); cur.execute(f"delete from {IDX_SCHEMA}.detail_control where ticket_id=%s",(tid,)); cur.close()
                    continue
                lu=t.get("lastUpdate")
                if isinstance(lu,str):
                    try: lu=datetime.fromisoformat(lu.replace("Z","+00:00"))
                    except: lu=None
                if lu and lu>watch.get(tid):
                    td=fetch_ticket_detail(tid)
                    if not td: continue
                    lr=latest_resolved_changed_date(td)
                    rows=dedupe_rows(extract_rows(td))
                    delete_ticket_rows(conn, tid)
                    upsert_detail(conn, rows)
                    upsert_control(conn, tid, lr, lu)
    conn.close()

if __name__ == "__main__":
    run()
