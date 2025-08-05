import os
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL = "https://api.movidesk.com/public/v1"
TOKEN   = os.environ["MOVIDESK_TOKEN"]
DSN     = os.environ["NEON_DSN"]

session = requests.Session()
session.headers.update({"token": TOKEN})

conn = psycopg2.connect(DSN)

def get_ticket_ids():
    return [274067]

def fetch_status_history(ticket_id):
    r = session.get(f"{API_URL}/tickets/statusHistory", params={"ticketId": ticket_id})
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()

def save_history(events, ticket_id):
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM visualizacao_resolucao.resolucao_por_status WHERE ticket_id = %s",
        (ticket_id,)
    )
    rows = []
    for ev in events:
        changed = ev.get("changedBy") or {}
        agent = changed.get("name") or changed.get("userName")
        team  = changed.get("teamName") or (changed.get("team") or {}).get("name")
        rows.append((
            ticket_id,
            ev.get("status"),
            ev.get("justificativa", ""),
            ev.get("secondsUtL", 0),
            ev.get("permanencyTimeFullTimeSeconds", 0.0),
            changed,
            ev.get("changedDate"),
            agent,
            team
        ))
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id, status, justificativa, seconds_utl, permanency_time_fulltime_seconds,
       changed_by, changed_date, agent_name, team_name)
    VALUES %s
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                       = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds  = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by                        = EXCLUDED.changed_by,
          changed_date                      = EXCLUDED.changed_date,
          agent_name                        = EXCLUDED.agent_name,
          team_name                         = EXCLUDED.team_name,
          imported_at                       = NOW();
    """
    execute_values(cur, sql, rows)
    conn.commit()

def main():
    for tid in get_ticket_ids():
        evs = fetch_status_history(tid)
        if evs:
            save_history(evs, tid)

if __name__ == "__main__":
    main()
