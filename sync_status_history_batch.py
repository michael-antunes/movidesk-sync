import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from time import sleep

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']

def api_get(url, params):
    for i in range(6):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            sleep(min(60, 2**i))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def ensure_structure():
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("create schema if not exists visualizacao_resolucao;")
    cur.execute("""
        create table if not exists visualizacao_resolucao.resolucao_por_status(
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
    cur.execute("""
        create table if not exists visualizacao_resolucao.sync_control(
            name text primary key,
            last_update timestamp not null
        );
    """)
    cur.execute("""
        select 1
        from information_schema.columns
        where table_schema='visualizacao_resolucao'
          and table_name='resolucao_por_status'
          and column_name='seconds_utl'
    """)
    if cur.fetchone():
        cur.execute("alter table visualizacao_resolucao.resolucao_por_status rename column seconds_utl to seconds_uti;")
    cur.close()
    conn.close()

def get_cursor():
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("select last_update from visualizacao_resolucao.sync_control where name='status_history_batch'")
    row = cur.fetchone()
    if row:
        cur.close(); conn.close()
        return row[0].replace(tzinfo=timezone.utc)
    dt = datetime.now(timezone.utc) - timedelta(days=7)
    cur = conn.cursor()
    cur.execute("insert into visualizacao_resolucao.sync_control(name,last_update) values('status_history_batch', %s) on conflict (name) do nothing", (dt,))
    cur.close(); conn.close()
    return dt

def set_cursor(ts):
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("update visualizacao_resolucao.sync_control set last_update=%s where name='status_history_batch'", (ts,))
    cur.close(); conn.close()

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def list_updated_ticket_ids(cursor_dt):
    ids = set()
    max_ts = cursor_dt
    def fetch(url):
        nonlocal ids, max_ts
        top = 1000
        skip = 0
        while True:
            params = {
                'token': MOVIDESK_TOKEN,
                '$select': 'id,lastUpdate,baseStatus',
                '$filter': f"lastUpdate ge {iso(cursor_dt)}",
                '$top': top,
                '$skip': skip
            }
            batch = api_get(url, params)
            if not batch:
                break
            for t in batch:
                tid = t.get('id')
                if tid is not None:
                    ids.add(int(tid))
                lu = t.get('lastUpdate')
                if lu:
                    try:
                        ts = datetime.fromisoformat(lu.replace('Z','+00:00'))
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        if ts > max_ts:
                            max_ts = ts
                    except Exception:
                        pass
            if len(batch) < top:
                break
            skip += len(batch)
    fetch('https://api.movidesk.com/public/v1/tickets')
    fetch('https://api.movidesk.com/public/v1/tickets/past')
    return list(ids), max_ts

def fetch_ticket_detail(ticket_id):
    params = {
        'token': MOVIDESK_TOKEN,
        'id': ticket_id,
        '$select': 'id,protocol,status,baseStatus,ownerTeam',
        '$expand': 'statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))'
    }
    return api_get('https://api.movidesk.com/public/v1/tickets/past', params)

def is_resolved(ticket):
    bs = (ticket or {}).get('baseStatus') or ''
    st = (ticket or {}).get('status') or ''
    if bs in ('Resolved','Closed'):
        return True
    if (st or '').lower() in ('resolved','closed','resolvido','fechado'):
        return True
    return False

def owner_team_name(ticket):
    ot = ticket.get('ownerTeam')
    if isinstance(ot, dict):
        return ot.get('businessName') or ''
    if isinstance(ot, str):
        return ot
    return ''

def teams_to_name(teams):
    names = []
    if isinstance(teams, list):
        for t in teams:
            if isinstance(t, dict) and t.get('businessName'):
                names.append(t['businessName'])
            elif isinstance(t, str):
                names.append(t)
    return ', '.join(sorted(set([n for n in names if n])))

def extract_rows(ticket):
    tid = ticket.get('id')
    protocol = ticket.get('protocol')
    owner_team = owner_team_name(ticket)
    rows = []
    for h in ticket.get('statusHistories', []) or []:
        status = h.get('status') or ''
        justification = h.get('justification') or ''
        sec_work = h.get('permanencyTimeWorkingTime') or 0
        sec_full = h.get('permanencyTimeFullTime') or 0
        changed_by = h.get('changedBy') or {}
        agent = changed_by.get('businessName') if isinstance(changed_by, dict) else ''
        team = teams_to_name(changed_by.get('teams')) if isinstance(changed_by, dict) else ''
        if not team:
            cbt = h.get('changedByTeam')
            if isinstance(cbt, dict) and cbt.get('businessName'):
                team = cbt['businessName']
            elif isinstance(cbt, str):
                team = cbt
        if not team:
            team = owner_team
        changed_date = h.get('changedDate')
        rows.append((tid, protocol, status, justification, int(sec_work), float(sec_full), json.dumps(changed_by, ensure_ascii=False), changed_date, agent, team))
    return rows

def delete_ticket_rows(conn, ticket_id):
    cur = conn.cursor()
    cur.execute("delete from visualizacao_resolucao.resolucao_por_status where ticket_id=%s", (ticket_id,))
    cur.close()

def upsert_rows(conn, rows):
    if not rows:
        return
    cur = conn.cursor()
    sql = """
        insert into visualizacao_resolucao.resolucao_por_status
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

def run():
    ensure_structure()
    cursor_dt = get_cursor()
    ids, max_ts = list_updated_ticket_ids(cursor_dt)
    if not ids:
        set_cursor(max_ts)
        return
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    for tid in ids:
        ticket = fetch_ticket_detail(tid)
        delete_ticket_rows(conn, tid)
        if is_resolved(ticket):
            rows = extract_rows(ticket)
            upsert_rows(conn, rows)
    conn.close()
    next_cursor = max_ts - timedelta(minutes=1)
    set_cursor(next_cursor)

if __name__ == '__main__':
    run()
