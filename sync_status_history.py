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
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data.get('statusHistories', [])

def save_to_db(events):
    conn = psycopg2.connect(NEON_DSN)
    cur = conn.cursor()
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id, protocol, status, justificativa, seconds_utl, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name)
    VALUES %s
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl       = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by         = EXCLUDED.changed_by,
          changed_date       = EXCLUDED.changed_date,
          agent_name         = EXCLUDED.agent_name,
          team_name          = EXCLUDED.team_name,
          imported_at        = NOW();
    """
    rows = []
    for ev in events:
        ch = ev.get('changedBy', {})
        team = ev.get('changedByTeam', {}) or {}
        rows.append((
            int(TICKET_ID),
            ev.get('protocol'),
            ev.get('status'),
            ev.get('justification', ''),
            ev.get('secondsInStatus'),
            ev.get('secondsFullTimeInStatus'),
            ev.get('changedBy'),
            ev.get('changedDate'),
            ch.get('businessName'),
            team.get('businessName')
        ))
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    evs = fetch_status_history(TICKET_ID)
    if evs:
        save_to_db(evs)
