import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values

MOVIEDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
TICKET_ID = os.environ['TICKET_ID']

def fetch_ticket_with_status_history(ticket_id):
    url = 'https://api.movidesk.com/public/v1/tickets/past'
    select_cols = 'id,protocol,status,baseStatus'
    expand_status = 'statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate;$expand=changedBy($select=businessName),changedByTeam($select=businessName))'
    params = {
        'token': MOVIEDESK_TOKEN,
        'id': ticket_id,
        '$select': select_cols,
        '$expand': expand_status
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def is_resolved(ticket):
    bs = (ticket or {}).get('baseStatus') or ''
    st = (ticket or {}).get('status') or ''
    return bs in ('Resolved', 'Closed') or st in ('Resolvido', 'Fechado', 'Resolved', 'Closed')

def rows_from_status_histories(ticket):
    ticket_id = ticket.get('id')
    protocol = ticket.get('protocol')
    rows = []
    for h in ticket.get('statusHistories', []) or []:
        status = h.get('status') or ''
        justification = h.get('justification') or ''
        sec_work = h.get('permanencyTimeWorkingTime') or 0
        sec_full = h.get('permanencyTimeFullTime') or 0.0
        changed_by = h.get('changedBy') or {}
        agent_name = (changed_by or {}).get('businessName') or ''
        team_name = ''
        if isinstance(h.get('changedByTeam'), dict):
            team_name = h.get('changedByTeam', {}).get('businessName') or ''
        changed_date = h.get('changedDate')
        rows.append((
            ticket_id,
            protocol,
            status,
            justification,
            int(sec_work),
            float(sec_full),
            json.dumps(changed_by, ensure_ascii=False),
            changed_date,
            agent_name,
            team_name
        ))
    return rows

def upsert_rows(rows):
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
    ticket = fetch_ticket_with_status_history(TICKET_ID)
    if not is_resolved(ticket):
        print('Ticket não está resolvido. Nada a fazer.')
        return
    rows = rows_from_status_histories(ticket)
    upsert_rows(rows)
    print(f'Upsert concluído para ticket {TICKET_ID} com {len(rows)} registros.')

if __name__ == '__main__':
    main()
