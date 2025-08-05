import os, time, requests, psycopg2
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://api.movidesk.com/public/v1"
TOKEN   = os.environ["MOVIDESK_TOKEN"]
DSN     = os.environ["NEON_DSN"]
HEADERS = {"token": TOKEN}

session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retry))

def fetch_team_name(team_id):
    if not team_id: return None
    r = session.get(f"{API_URL}/teams/{team_id}", headers=HEADERS)
    return r.json().get("name") if r.status_code == 200 else None

def fetch_status_history(ticket_id):
    r = session.get(f"{API_URL}/tickets/statusHistory", headers=HEADERS, params={"ticketId": ticket_id})
    return [] if r.status_code == 404 else r.json()

def save_to_db(ticket_id, history):
    if not history: return
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()
    rows = []
    for ev in history:
        cb = ev.get("changedBy") or {}
        rows.append((
            ticket_id,
            ev.get("status"),
            ev.get("justification") or "",
            ev.get("permanencyTimeWorkingTime"),
            ev.get("permanencyTimeFullTime"),
            cb,
            ev.get("changedDate"),
            cb.get("name"),
            fetch_team_name(cb.get("teamId"))
        ))
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
    (ticket_id, status, justificativa, seconds_utl, permanency_time_fulltime_seconds,
     changed_by, changed_date, agent_name, team_name, imported_at)
    VALUES %s
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                       = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by                        = EXCLUDED.changed_by,
          changed_date                      = EXCLUDED.changed_date,
          agent_name                        = EXCLUDED.agent_name,
          team_name                         = EXCLUDED.team_name,
          imported_at                       = NOW()
    ;
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    tid = 274067
    hist = fetch_status_history(tid)
    save_to_db(tid, hist)

if __name__ == "__main__":
    main()
