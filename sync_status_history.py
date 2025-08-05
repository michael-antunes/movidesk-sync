#!/usr/bin/env python3
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

API_URL   = "https://api.movidesk.com/public/v1/tickets/past"
TOKEN     = os.environ["MOVIDESK_TOKEN"]
DSN       = os.environ["NEON_DSN"]
TICKET_ID = os.environ.get("TICKET_ID")

if not TICKET_ID:
    raise SystemExit("❌ Defina TICKET_ID ao rodar o script")

def fetch_status_history(ticket_id):
    params = {
        "token": TOKEN,
        "id": ticket_id,
        "$expand": "statusHistories("
                   "$expand=changedBy($select=businessName),"
                   "changedByTeam($select=businessName))"
    }
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    data = r.json()
    return data.get("statusHistories", [])

def save_to_db(records):
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()
    cur.execute(
        "DELETE FROM visualizacao_resolucao.resolucao_por_status WHERE ticket_id = %s",
        (TICKET_ID,)
    )
    if records:
        rows = []
        for ev in records:
            agent = ev.get("changedBy", {}).get("businessName") or ""
            team  = ev.get("changedByTeam", {}).get("businessName") or ""
            changed_dt = None
            cd = ev.get("changedDate")
            if cd:
                try:
                    changed_dt = datetime.fromisoformat(cd)
                except:
                    changed_dt = None
            rows.append((
                int(TICKET_ID),
                ev.get("status"),
                ev.get("justification") or "",
                ev.get("permanencyTimeWorkingTime"),
                ev.get("permanencyTimeFullTime"),
                agent,
                team,
                changed_dt
            ))
        sql = """
        INSERT INTO visualizacao_resolucao.resolucao_por_status
          (ticket_id, status, justificativa,
           seconds_utl, permanency_time_fulltime_seconds,
           agent_name, team_name, changed_date)
        VALUES %s
        ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
          SET
            seconds_utl                       = EXCLUDED.seconds_utl,
            permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
            agent_name                       = EXCLUDED.agent_name,
            team_name                        = EXCLUDED.team_name,
            changed_date                     = EXCLUDED.changed_date,
            imported_at                      = NOW()
        ;
        """
        execute_values(cur, sql, rows)
    conn.commit()
    conn.close()

def main():
    history = fetch_status_history(TICKET_ID)
    save_to_db(history)
    print(f"✅ Importados {len(history)} eventos para o ticket {TICKET_ID}")

if __name__ == "__main__":
    main()
