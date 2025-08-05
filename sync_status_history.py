import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter, Retry

API_URL = "https://api.movidesk.com/public/v1"
TOKEN   = os.getenv("MOVIDESK_TOKEN")
DSN     = os.getenv("NEON_DSN")
HEADERS = {"token": TOKEN}

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

def fetch_team_name(team_id):
    if not team_id:
        return None
    r = session.get(f"{API_URL}/teams/{team_id}", headers=HEADERS)
    return r.json().get("name") if r.status_code == 200 else None

def fetch_status_history(ticket_id):
    r = session.get(
        f"{API_URL}/tickets/statusHistory",
        headers=HEADERS,
        params={"ticketId": ticket_id},
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()

def save_history(ticket_id, history):
    if not history:
        return

    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()

    rows = []
    for ev in history:
        changed_by = ev.get("changedBy") or {}
        rows.append((
            ticket_id,
            ev.get("status"),
            ev.get("justification") or "",
            ev.get("permanencyTimeWorkingTime"),
            ev.get("permanencyTimeFullTime"),
            changed_by,
            ev.get("changedDate"),
            changed_by.get("name"),
            fetch_team_name(changed_by.get("teamId")),
        ))

    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status (
      ticket_id,
      status,
      justificativa,
      seconds_utl,
      permanency_time_fulltime_seconds,
      changed_by,
      changed_date,
      agent_name,
      team_name
    )
    VALUES %s
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                       = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by                       = EXCLUDED.changed_by,
          changed_date                     = EXCLUDED.changed_date,
          agent_name                       = EXCLUDED.agent_name,
          team_name                        = EXCLUDED.team_name,
          imported_at                      = NOW()
    ;
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    ticket_ids = [274067]
    for tid in ticket_ids:
        history = fetch_status_history(tid)
        save_history(tid, history)

if __name__ == "__main__":
    main()
