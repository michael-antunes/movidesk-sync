import os
import requests
import psycopg2
from psycopg2.extras import execute_values

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN       = os.environ['NEON_DSN']
TICKET_ID      = os.environ.get('TICKET_ID') or None

def fetch_status_history(ticket_id):
    url = 'https://api.movidesk.com/public/v1/tickets/past'
    params = {
        'id': ticket_id,
        'token': MOVIDESK_TOKEN,
        '$expand': 'statusHistories('
                   '$expand=changedBy($select=businessName),'
                   'changedByTeam($select=businessName)'
                   ')'
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json().get('statusHistories', [])

def fetch_all_ticket_ids():
    conn = psycopg2.connect(NEON_DSN)
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT ticket_id FROM tickets_movidesk;')
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids

def save_to_db(events):
    conn = psycopg2.connect(NEON_DSN)
    cur = conn.cursor()
    cur.execute('TRUNCATE visualizacao_resolucao.resolucao_por_status;')
    rows = []
    for ev in events:
        rows.append([
            ev['ticketId'],
            ev.get('protocol'),
            ev['status'],
            ev.get('justification'),
            ev.get('secondsUtil'),
            ev.get('permanencyTimeFulltimeSeconds'),
            ev.get('changedBy', {}).get('businessName'),
            ev.get('changedByTeam', {}).get('businessName'),
            ev.get('changedDate')
        ])
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id, protocol, status, justificativa,
       seconds_utl, permanency_time_fulltime_seconds,
       agent_name, team_name, changed_date)
    VALUES %s
    ON CONFLICT(ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                        = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds   = EXCLUDED.permanency_time_fulltime_seconds,
          agent_name                         = EXCLUDED.agent_name,
          team_name                          = EXCLUDED.team_name,
          changed_date                       = EXCLUDED.changed_date,
          imported_at                        = NOW();
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    if TICKET_ID:
        ticket_ids = [int(TICKET_ID)]
    else:
        ticket_ids = fetch_all_ticket_ids()

    all_events = []
    for tid in ticket_ids:
        evs = fetch_status_history(tid)
        for e in evs:
            all_events.append(e)

    save_to_db(all_events)

if __name__ == '__main__':
    main()
