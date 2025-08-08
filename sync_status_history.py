import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from time import sleep

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
TICKET_ID = os.environ.get('TICKET_ID', '').strip()

def api_get(url, params):
    for i in range(5):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            sleep(2**i)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def fetch_ticket_past(ticket_id):
    url = 'https://api.movidesk.com/public/v1/tickets/past'
    params = {
        'token': MOVIDESK_TOKEN,
        'id': ticket_id,
        '$select': 'id,protocol,status,baseStatus,ownerTeam',
        '$expand': 'statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;$expand=changedBy($select=id,businessName;$expand=teams($select=businessName)))'
    }
    return api_get(url, params)

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
            primary key (ticket_id, status, justificativa)
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

def get_owner_team(ticket):
    ot = ticket.get('ownerTeam')
    if isinstance(ot, dict):
        return ot.get('businessName') or ''
    if isinstance(ot, str):
        return ot
    return ''

def is_resolved(ticket):
    bs = (ticket or {}).get('baseStatus') or ''
    st = (ticket or {}).get('status') or ''
    if bs in ('Resolved', 'Closed'):
        return True
    if (st or '').lower() in ('resolvido', 'fechado', 'resolved', 'closed'):
        return True
    for h in ticket.get('statusHistories', []) or []:
        s = (h.get('status') or '').lower()
        if s in ('resolvido', 'fechado', 'resolved', 'closed'):
            return True
    return False

def teams_to_name(teams):
    names = []
    if isinstance(teams, list):
        for t in teams:
            if isinstance(t, dict):
                n = t.get('businessName')
                if n:
                    names.append(n)
            elif isinstance(t, str):
                names.append(t)
    return ', '.join(sorted(set([n for n in names if n])))

def extract_rows(ticket):
    tid = ticket.get('id')
    protocol = ticket.get('protocol')
    owner_team = get_owner_team(ticket)
    rows = []
    for h in ticket.get('statusHistories', []) or []:
        status = h.get('status') or ''
        justification = h.get('justification') or ''
        sec_work = h.get('permanencyTimeWorkingTime') or 0
        sec_full = h.get('permanencyTimeFullTime') or 0
        changed_by = h.get('changedBy') or {}
        agent = ''
        if isinstance(changed_by, dict):
            agent = changed_by.get('businessName') or ''
            team = teams_to_name(changed_by.get('teams'))
        else:
            team = ''
        if not team:
            cbt = h.get('changedByTeam')
            if isinstance(cbt, dict):
                team = cbt.get('businessName') or ''
            elif isinstance(cbt, str):
                team = cbt
        if not team:
            team = owner_team
        changed_date = h.get('changedDate')
        rows.append((tid, protocol, status, justification, int(sec_work), float(sec_full), json.dumps(changed_by, ensure_ascii=False), changed_date, agent, team))
    return rows

def upsert(rows):
    if not rows:
        return
    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
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
            team_name = excluded.team_name;
    """
    execute_values(cur, sql, rows)
    cur.close()
    conn.close()

def main():
    if not TICKET_ID:
        raise SystemExit('TICKET_ID ausente')
    ensure_structure()
    ticket = fetch_ticket_past(TICKET_ID)
    if not is_resolved(ticket):
        raise SystemExit('Ticket não está resolvido')
    rows = extract_rows(ticket)
    upsert(rows)
    print(len(rows))

if __name__ == '__main__':
    main()
