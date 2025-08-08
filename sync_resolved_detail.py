import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from time import sleep

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
IDX_SCHEMA = 'visualizacao_resolvidos'
DET_SCHEMA = 'visualizacao_resolucao'
RESOLVED_SET = {'resolved','closed','resolvido','fechado'}

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
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"create schema if not exists {DET_SCHEMA};")
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
        create table if not exists {IDX_SCHEMA}.detail_control(
            ticket_id integer primary key,
            last_resolved_at timestamp,
            last_update timestamp,
            synced_at timestamp
        );
    """)
    cur.close(); conn.close()

def fetch_ticket_detail(ticket_id):
    sel='id,protocol,status,baseStatus,ownerTeam'
    exp='statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))'
    p={'token':MOVIDESK_TOKEN,'id':ticket_id,'$select':sel,'$expand':exp}
    try:
        return api_get('https://api.movidesk.com/public/v1/tickets', p)
    except requests.exceptions.HTTPError as e:
        if e.response is None or e.response.status_code != 404: raise
    return api_get('https://api.movidesk.com/public/v1/tickets/past', p)

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

def delete_ticket_rows(conn, tid):
    cur=conn.cursor(); cur.execute(f"delete from {DET_SCHEMA}.resolucao_por_status where ticket_id=%s",(tid,)); cur.close()

def upsert_detail(conn, rows):
    if not rows: return
    cur=conn.cursor()
    sql=f"""
        insert into {DET_SCHEMA}.resolucao_por_status
        (ticket_id, protocol, status, justificativa, seconds_uti, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
        values %s
        on conflict (ticket_id, status, justificativa) do update set
            protocol=excluded.protocol,
            seconds_uti=excluded.seconds_uti,
            permanency_time_fulltime_seconds=excluded.permanency_time_fulltime_seconds,
            changed_by=excluded.changed_by,
            changed_date=excluded.changed_date,
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

def run():
    ensure_structure()
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute(f"""
        select t.ticket_id, t.last_resolved_at, t.last_update
        from {IDX_SCHEMA}.tickets_resolvidos t
        left join {IDX_SCHEMA}.detail_control d on d.ticket_id=t.ticket_id
        where d.ticket_id is null
           or t.last_resolved_at is distinct from d.last_resolved_at
           or t.last_update     is distinct from d.last_update
    """)
    to_sync=cur.fetchall()
    cur.execute(f"""
        select distinct h.ticket_id
        from {DET_SCHEMA}.resolucao_por_status h
        left join {IDX_SCHEMA}.tickets_resolvidos t on t.ticket_id=h.ticket_id
        where t.ticket_id is null
    """)
    to_delete=[r[0] for r in cur.fetchall()]
    cur.close()
    for tid,lr,lu in to_sync:
        try:
            ticket=fetch_ticket_detail(tid)
        except Exception as e:
            print("skip", tid, str(e)); continue
        delete_ticket_rows(conn, tid)
        rows=dedupe_by_pk_keep_latest(extract_rows(ticket))
        upsert_detail(conn, rows)
        upsert_control(conn, tid, lr, lu)
    if to_delete:
        cur=conn.cursor()
        execute_values(cur, f"delete from {DET_SCHEMA}.resolucao_por_status where ticket_id in %s", [tuple(to_delete)])
        cur.close()
    conn.close()

if __name__ == '__main__':
    run()
