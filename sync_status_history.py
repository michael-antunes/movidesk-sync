import os
import requests
import psycopg2
from psycopg2.extras import execute_values

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']
TICKET_ID = os.environ['TICKET_ID']

def fetch_status_history(ticket_id):
    url = 'https://api.movidesk.com/public/v1/tickets/past'
    params = {
        'id': ticket_id,
        'token': MOVIDESK_TOKEN,
        '$expand': 'statusHistories($expand=changedBy($select=businessName),changedByTeam($select=businessName))'
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get('statusHistories', [])

def save_to_db(events):
    conn = psycopg2.connect(NEON_DSN)
    cur = conn.cursor()
    cur.execute('TRUNCATE visualizacao_resolucao.resolucao_por_status')
    rows = []
    for ev in events:
        rows.append([
            ev.get('ticketId'),
            ev.get('protocol'),
            ev.get('status'),
            ev.get('justification', ''),
            ev.get('secondsUtc'),
            ev.get('permanencyTimeFulltimeSeconds'),
            ev.get('changedBy'),
            ev.get('changedDate'),
            ev.get('changedBy', {}).get('businessName'),
            ev.get('changedByTeam', {}).get('businessName'),
        ])
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id, protocol, status, justificativa, seconds_utl,
       permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
    VALUES %s
    ON CONFLICT(ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                          = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds     = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by                           = EXCLUDED.changed_by,
          changed_date                         = EXCLUDED.changed_date,
          agent_name                           = EXCLUDED.agent_name,
          team_name                            = EXCLUDED.team_name,
          imported_at                          = NOW();
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    events = fetch_status_history(TICKET_ID)
    save_to_db(events)

if __name__ == '__main__':
    main()
