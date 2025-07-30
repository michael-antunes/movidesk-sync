import os
import json
import requests
import psycopg2

API_URL    = "https://api.movidesk.com/public/v1"
MOVI_TOKEN = os.environ["MOVIDESK_TOKEN"]
NEON_DSN   = os.environ["NEON_DSN"]

SCHEMA = "visualizacao_resolucao"
TABLE  = f"{SCHEMA}.resolucao_por_status"

HEADERS = {
    "Content-Type": "application/json",
    "token": MOVI_TOKEN,
}

def get_ticket_ids():
    sql = f"""
    SELECT DISTINCT ticket_id
      FROM {TABLE}
    """
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]

def fetch_status_history(ticket_id):
    url = f"{API_URL}/tickets/statusHistory?ticketId={ticket_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def save_to_db(ticket_id, protocol, history):
    if not history:
        return
    rows = []
    for ev in history:
        rows.append((
            ticket_id,
            protocol,
            ev.get("status"),
            ev.get("justification") or "-",
            ev.get("secondsUtl", 0),
            ev.get("permanencyTimeFulltimeSeconds"),
            json.dumps(ev.get("changedBy")) if ev.get("changedBy") else None,
            ev.get("changedDate"),
            ev.get("agent",{}).get("name"),
            ev.get("team",{}).get("name"),
        ))

    insert_sql = f"""
    INSERT INTO {TABLE}
      (ticket_id, protocol, status, justificativa,
       seconds_utl, permanency_time_fulltime_seconds,
       changed_by, changed_date,
       agent_name, team_name, imported_at)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
    ON CONFLICT (ticket_id, status, justificativa) DO NOTHING
    ;
    """
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
        conn.commit()

def main():
    tickets = get_ticket_ids()
    for tid in tickets:
        history = fetch_status_history(tid)
        save_to_db(tid, None, history)

if __name__ == "__main__":
    main()
