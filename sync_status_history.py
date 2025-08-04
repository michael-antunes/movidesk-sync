import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import date

API_URL = "https://api.movidesk.com/public/v1"
TOKEN   = os.environ["MOVIDESK_TOKEN"]
DSN     = os.environ["NEON_DSN"]
HEADERS = {"token": TOKEN}

def get_ticket_ids():
    today = date.today().isoformat()
    ids = []
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "$select": "id",
            "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {today}",
            "$top": 100,
            "$skip": skip
        }
        r = requests.get(f"{API_URL}/tickets", params=params)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") if isinstance(data, dict) else data
        if not items:
            break
        ids.extend(i["id"] for i in items)
        if len(items) < 100:
            break
        skip += 100
    return ids

def fetch_status_history(ticket_id):
    params = {
        "token": TOKEN,
        "id": ticket_id,
        "$expand": (
            "statusHistories("
            "$select=status,justification,"
            "permanencyTimeWorkingTime,permanencyTimeFullTime,"
            "changedBy,changedDate)"
        )
    }
    r = requests.get(f"{API_URL}/tickets", params=params)
    r.raise_for_status()
    data = r.json()
    items = data.get("items") if isinstance(data, dict) else data
    return items[0].get("statusHistories", []) if items else []

def save_to_db(ticket_id, history):
    if not history:
        return
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    rows = []
    for ev in history:
        changed_by = ev.get("changedBy")
        rows.append((
            ticket_id,
            ev.get("protocol"),
            ev.get("status"),
            ev.get("justification") or "",
            ev.get("permanencyTimeFullTime"),
            ev.get("permanencyTimeWorkingTime"),
            changed_by,
            ev.get("changedDate"),
            changed_by.get("name") if changed_by else None,
            changed_by.get("team", {}).get("name") if changed_by else None
        ))
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id,protocol,status,justificativa,
       seconds_utl,permanency_time_fulltime_seconds,
       changed_by,changed_date,agent_name,team_name,imported_at)
    VALUES %s
    ON CONFLICT (ticket_id,status,justificativa) DO UPDATE
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
