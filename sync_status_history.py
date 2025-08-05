import os
import sys
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

TOKEN     = os.environ.get("MOVIDESK_TOKEN", "").strip()
DSN       = os.environ.get("NEON_DSN",    "").strip()
TICKET_ID = os.environ.get("TICKET_ID",   "").strip()

if not TOKEN or not DSN or not TICKET_ID:
    print("❌ Faltando variável de ambiente. Exemplo:")
    print("   MOVIDESK_TOKEN, NEON_DSN e TICKET_ID devem estar definidos.")
    sys.exit(1)

print(f"→ Usando TICKET_ID = {TICKET_ID}")

API_BASE = "https://api.movidesk.com/public/v1"
session  = requests.Session()
session.headers.update({"token": TOKEN})

def fetch_status_history(ticket_id):
    params = {
        "id":       ticket_id,
        "$expand": "statusHistories("
                   "changedBy($select=businessName),"
                   "changedByTeam($select=businessName))"
    }
    r = session.get(f"{API_BASE}/tickets", params=params)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        return []
    return items[0].get("statusHistories", [])

def save_history(ticket_id, records):
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()
    cur.execute(
        "DELETE FROM visualizacao_resolucao.resolucao_por_status WHERE ticket_id = %s",
        (ticket_id,)
    )
    if records:
        rows = []
        seen = set()
        for ev in records:
            status       = ev.get("status", "")
            justification= ev.get("justification") or ""
            key          = (status, justification)
            if key in seen:
                continue
            seen.add(key)

            agent = ev.get("changedBy", {}).get("businessName") or ""
            team  = ev.get("changedByTeam", {}).get("businessName") or ""
            cd = ev.get("changedDate")
            try:
                changed_date = datetime.fromisoformat(cd) if cd else None
            except:
                changed_date = None

            rows.append((
                int(ticket_id),
                status,
                justification,
                ev.get("permanencyTimeWorkingTime"),
                ev.get("permanencyTimeFullTime"),
                agent,
                team,
                changed_date
            ))

        execute_values(
            cur,
            """
            INSERT INTO visualizacao_resolucao.resolucao_por_status
              (ticket_id, status, justificativa,
               seconds_utl, permanency_time_fulltime_seconds,
               agent_name, team_name, changed_date)
            VALUES %s
            ON CONFLICT (ticket_id,status,justificativa) DO UPDATE
              SET
                seconds_utl                       = EXCLUDED.seconds_utl,
                permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
                agent_name                       = EXCLUDED.agent_name,
                team_name                        = EXCLUDED.team_name,
                changed_date                     = EXCLUDED.changed_date,
                imported_at                      = NOW()
            ;
            """,
            rows
        )
    conn.commit()
    conn.close()

def main():
    history = fetch_status_history(TICKET_ID)
    print(f"→ Encontrados {len(history)} eventos em statusHistories")
    save_history(TICKET_ID, history)
    print(f"✅ Histórico de status do ticket {TICKET_ID} sincronizado.")

if __name__ == "__main__":
    main()
