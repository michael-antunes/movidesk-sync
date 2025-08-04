import os
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL = "https://api.movidesk.com/public/v1"
TOKEN   = os.environ["MOVIDESK_TOKEN"]
DSN     = os.environ["NEON_DSN"]
HEADERS = {"token": TOKEN}

def get_ticket_ids():
    skip = 0
    ids = []
    while True:
        params = {
            "token": TOKEN,
            "$select": "id",
            "$top": 100,
            "$skip": skip
        }
        r = requests.get(f"{API_URL}/tickets", params=params)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        ids.extend(t["id"] for t in batch)
        skip += 100
    return ids

def fetch_team_name(team_id):
    if not team_id:
        return None
    r = requests.get(f"{API_URL}/teams/{team_id}", headers=HEADERS)
    if r.status_code == 200:
        return r.json().get("name")
    return None

def fetch_status_history(ticket_id):
    r = requests.get(
        f"{API_URL}/tickets/statusHistory",
        params={"ticketId": ticket_id, "token": TOKEN},
        headers=HEADERS
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()  # já é um array de eventos

def save_to_db(ticket_id, history):
    if not history:
        return
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    rows = []
    for ev in history:
        changed_by = ev.get("changedBy") or {}
        team = fetch_team_name(changed_by.get("teamId"))
        rows.append((
            ticket_id,
            ev.get("status"),
            ev.get("justification") or "",
            ev.get("permanencyTimeWorkingTime"),
            ev.get("permanencyTimeFullTime"),
            changed_by,
            ev.get("changedDate"),
            changed_by.get("name"),
            team
        ))
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id, status, justificativa,
       seconds_utl, permanency_time_fulltime_seconds,
       changed_by, changed_date, agent_name, team_name, imported_at)
    VALUES %s
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET
        seconds_utl                       = EXCLUDED.seconds_utl,
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
    for tid in get_ticket_ids():
        hist = fetch_status_history(tid)
        save_to_db(tid, hist)

if __name__ == "__main__":
    main()
