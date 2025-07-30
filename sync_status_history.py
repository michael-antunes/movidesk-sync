#!/usr/bin/env python3
import os
import requests
import psycopg2
from psycopg2.extras import execute_values

MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]
NEON_DSN = os.environ["NEON_DSN"]
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {MOVIDESK_TOKEN}"
}
STATUS_HISTORY_URL = "https://api.movidesk.com/public/v1/tickets/statusHistory"

def get_ticket_ids():
    conn = psycopg2.connect(NEON_DSN)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticket_id FROM visualizacao_resolucao.resolucao_por_status")
    ids = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return ids

def fetch_status_history(ticket_id):
    resp = requests.get(f"{STATUS_HISTORY_URL}?ticketId={ticket_id}", headers=HEADERS)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json()

def save_to_db(ticket_id, history):
    if not history:
        return
    conn = psycopg2.connect(NEON_DSN)
    cur  = conn.cursor()
    rows = []
    for ev in history:
        rows.append((
            ticket_id,
            ev.get("protocol"),
            ev.get("status"),
            ev.get("justification") or "",
            ev.get("secondsUTL"),
            ev.get("permanencyTimeFullTimeSeconds"),
            ev.get("changedBy"),
            ev.get("date"),
            ev.get("agentName"),
            ev.get("teamName"),
        ))
    sql = """
    INSERT INTO visualizacao_resolucao.resolucao_por_status
      (ticket_id,protocol,status,justificativa,seconds_utl,permanency_time_fulltime_seconds,changed_by,changed_date,agent_name,team_name)
    VALUES %s
    ON CONFLICT (ticket_id,status,justificativa) DO UPDATE
      SET
        seconds_utl                          = EXCLUDED.seconds_utl,
        permanency_time_fulltime_seconds    = EXCLUDED.permanency_time_fulltime_seconds,
        changed_by                          = EXCLUDED.changed_by,
        changed_date                        = EXCLUDED.changed_date,
        agent_name                          = EXCLUDED.agent_name,
        team_name                           = EXCLUDED.team_name,
        imported_at                         = NOW()
    ;
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

def main():
    for tid in get_ticket_ids():
        history = fetch_status_history(tid)
        save_to_db(tid, history)

if __name__ == "__main__":
    main()
