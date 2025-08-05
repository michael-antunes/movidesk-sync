import os
import requests
import psycopg2
from psycopg2.extras import execute_values

token = os.environ['MOVIDESK_TOKEN']
dsn = os.environ['NEON_DSN']
TICKET_ID = 274067
API_BASE = 'https://api.movidesk.com/public/v1'

def fetch_status_history(ticket_id):
    url = f"{API_BASE}/tickets/past"
    params = {
        'token': token,
        'id': ticket_id,
        '$expand': 'statusHistories($expand=changedBy($select=businessName),changedByTeam($select=businessName))'
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data.get('statusHistories', [])

def save_to_db(events):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("TRUNCATE visualizacao_resolucao.resolucao_por_status;")
    sql = "INSERT INTO visualizacao_resolucao.resolucao_por_status (ticket_id, protocol, status, justificativa, seconds_utl, permanency_time_fulltime_seconds, changed_by, changed_date, agent_name, team_name) VALUES %s"
    rows = []
    for ev in events:
        rows.append((
            TICKET_ID,
            None,
            ev.get('status'),
            ev.get('justification') or ev.get('justificativa') or '',
            ev.get('secondsUtl'),
            ev.get('permanencyTimeFulltimeSeconds'),
            None,
            ev.get('changedDate'),
            ev.get('changedBy', {}).get('businessName'),
            ev.get('changedByTeam', {}).get('businessName'),
        ))
    if rows:
        execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    evs = fetch_status_history(TICKET_ID)
    save_to_db(evs)

if __name__ == '__main__':
    main()
