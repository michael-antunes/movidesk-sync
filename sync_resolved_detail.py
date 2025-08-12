import os, json, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from time import sleep

TOKEN=os.environ["MOVIDESK_TOKEN"]
DSN=os.environ["NEON_DSN"]
IDX="visualizacao_resolvidos"
DET="visualizacao_resolucao"
BASE="https://api.movidesk.com/public/v1"
RESOLVED={"resolved","closed","resolvido","fechado"}
GROUP=24

def api_get(u,p,tries=6):
    for i in range(tries):
        r=requests.get(u,params=p,timeout=60)
        if r.status_code in (429,500,502,503,504):
            sleep(min(60,2**i)); continue
        r.raise_for_status(); return r.json()
    r.raise_for_status()

def ensure_structure():
    conn=psycopg2.connect(DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"create schema if not exists {DET};")
    cur.execute(f"""
        create table if not exists {DET}.resolucao_por_status(
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
          primary key(ticket_id,status,justificativa,changed_date)
        );
    """)
    cur.execute(f"""
        create table if not exists {IDX}.detail_control(
          ticket_id integer primary key,
          last_update timestamp,
          synced_at timestamp
        );
    """)
    cur.close(); conn.close()

def fetch_min_batch(id_batch):
    parts=[f"id eq {i}" for i in id_batch]
    filt="(" + " or ".join(parts) + ")"
    p={"token":TOKEN,"$select":"id,status,baseStatus,lastUpdate","$filter":filt,"$top":len(id_batch),"$skip":0}
    try:
        data=api_get(f"{BASE}/tickets",p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code!=404: raise
        data=[]
    return data or []

def fetch_detail(ticket_id):
    sel='id,protocol,status,baseStatus,ownerTeam,lastUpdate'
    exp='statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))'
    p={'token':TOKEN,'id':ticket_id,'$select':sel,'$expand':exp}
    try:
        return api_get(f"{BASE}/tickets",p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code!=404: raise
    return api_get(f"{BASE}/tickets/past",p)

def owner_team_name(t):
    ot=t.get('ownerTeam')
    if isinstance(ot,dict): return ot.get('businessName') or ''
    if isinstance(ot,str): return ot
    return ''

GENERIC={'administradores','agente administrador','administrators','agent administrator','default','geral','todos','all','users','usuários','colaboradores'}
PRI=['telefone','chat','n1','n2','cs','suporte','service desk','desenvolvimento','squad','projeto']

def pick_team(changed_by, owner_team, changed_by_team):
    name=''
    if isinstance(changed_by_team, dict): name=(changed_by_team.get('businessName') or '').strip()
    elif isinstance(changed_by_team, str): name=changed_by_team.strip()
    if name: return name
    names=[]
    if isinstance(changed_by, dict):
        teams=changed_by.get('teams')
        if isinstance(teams,list):
            for t in teams:
                n=(t.get('businessName') if isinstance(t,dict) else (t if isinstance(t,str) else '')) or ''
                n=n.strip()
                if n and n.lower() not in GENERIC and not n.lower().startswith('admin'):
                    names.append(n)
    names=list(dict.fromkeys(names))
    if not names and isinstance(changed_by, dict):
        teams=changed_by.get('teams')
        if isinstance(teams,list):
            for t in teams:
                n=(t.get('businessName') if isinstance(t,dict) else (t if isinstance(t,str) else '')) or ''
                n=n.strip()
                if n: names.append(n)
        names=list(dict.fromkeys(names))
    if names:
        lowered=[n.lower() for n in names]
        for key in PRI:
            for i,low in enumerate(lowered):
                if key in low: return names[i]
        return names[0]
    return owner_team or ''

def extract_rows(t):
    tid=t.get('id'); protocol=t.get('protocol'); owner=owner_team_name(t); rows=[]
    for h in t.get('statusHistories',[]) or []:
        status=h.get('status') or ''
        justification=h.get('justification') or ''
        sec_work=h.get('permanencyTimeWorkingTime') or 0
        sec_full=h.get('permanencyTimeFullTime') or 0
        changed_by=h.get('changedBy') or {}
        agent=changed_by.get('businessName') if isinstance(changed_by,dict) else ''
        team=pick_team(changed_by, owner, h.get('changedByTeam'))
        changed_date=h.get('changedDate')
        rows.append((tid,protocol,status,justification,int(sec_work),float(sec_full),json.dumps(changed_by,ensure_ascii=False),changed_date,agent,team))
    return rows

def dedupe(rows):
    seen=set(); out=[]
    for r in rows:
        k=(r[0],r[2],r[3],r[7])
        if k in seen: continue
        seen.add(k); out.append(r)
    return out

def ids_from_index():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id from {IDX}.tickets_resolvidos")
    data=[r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return data

def control_map():
    conn=psycopg2.connect(DSN); cur=conn.cursor()
    cur.execute(f"select ticket_id, last_update from {IDX}.detail_control")
    d={r[0]:r[1] for r in cur.fetchall()}
    cur.close(); conn.close()
    return d

def delete_missing(conn, keep):
    cur=conn.cursor()
    if keep:
        cur.execute(f"delete from {DET}.resolucao_por_status where ticket_id not in (select ticket_id from {IDX}.tickets_resolvidos)")
        cur.execute(f"delete from {IDX}.detail_control        where ticket_id not in (select ticket_id from {IDX}.tickets_resolvidos)")
    else:
        cur.execute(f"truncate table {DET}.resolucao_por_status")
        cur.execute(f"truncate table {IDX}.detail_control")
    cur.close()

def upsert_detail(conn, rows):
    if not rows: return
    cur=conn.cursor()
    sql=f"""
        insert into {DET}.resolucao_por_status
        (ticket_id, protocol, status, justificativa, seconds_uti, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
        values %s
        on conflict (ticket_id,status,justificativa,changed_date) do update set
          protocol=excluded.protocol,
          seconds_uti=excluded.seconds_uti,
          permanency_time_fulltime_seconds=excluded.permanency_time_fulltime_seconds,
          changed_by=excluded.changed_by,
          agent_name=excluded.agent_name,
          team_name=excluded.team_name,
          imported_at=now();
    """
    execute_values(cur, sql, rows); cur.close()

def upsert_control(conn, tid, lu):
    cur=conn.cursor()
    cur.execute(f"""
        insert into {IDX}.detail_control(ticket_id,last_update,synced_at)
        values (%s,%s,now())
        on conflict (ticket_id) do update set
          last_update=excluded.last_update,
          synced_at=now();
    """,(tid,lu))
    cur.close()

def run():
    ensure_structure()
    ids=ids_from_index()
    if not ids:
        conn=psycopg2.connect(DSN); conn.autocommit=True
        delete_missing(conn, False); conn.close()
        return
    ctrl=control_map()
    conn=psycopg2.connect(DSN); conn.autocommit=True
    changed=[]
    for i in range(0,len(ids),GROUP):
        batch=ids[i:i+GROUP]
        minis=fetch_min_batch(batch)
        for t in minis:
            tid=int(t["id"])
            lu=t.get("lastUpdate")
            if isinstance(lu,str):
                try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
                except Exception: lu=None
            prev=ctrl.get(tid)
            if prev is None or (lu and prev and lu>prev) or (lu and prev is None):
                changed.append(tid)
    for tid in changed or []:
        try:
            td=fetch_detail(tid)
        except Exception:
            continue
        rows=dedupe(extract_rows(td))
        cur=conn.cursor()
        cur.execute(f"delete from {DET}.resolucao_por_status where ticket_id=%s",(tid,))
        cur.close()
        upsert_detail(conn, rows)
        lu=td.get("lastUpdate")
        if isinstance(lu,str):
            try: lu=datetime.fromisoformat(lu.replace('Z','+00:00'))
            except Exception: lu=None
        upsert_control(conn, tid, lu)
    delete_missing(conn, True)
    conn.close()

if __name__=="__main__":
    run()
