import os
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL   = "https://api.movidesk.com/public/v1/tickets/past"
TOKEN     = os.environ["MOVIDESK_TOKEN"]
TICKET_ID = os.environ.get("TICKET_ID")

if not TICKET_ID:
    raise SystemExit("❌ Defina o ticket_id ao disparar o workflow")

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
    conn = psycopg2.connect(os.environ["NEON_DSN"])
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM visualizacao_resolucao.resolucao_por_status WHERE ticket_id = %s",
                (TICKET_ID,)
            )
            if not records:
                return

            rows = []
            for ev in records:
                agent = ev.get("changedBy", {}).get("businessName") or ""
                team  = ev.get("changedByTeam", {}).get("businessName") or ""
                rows.append((
                    int(TICKET_ID),
                    ev.get("ticketId"),                     # protocol
                    ev.get("status"),
                    ev.get("justification") or "",
                    ev.get("secondsUntilNextStatus"),
                    ev.get("permanencyTimeFullTimeSeconds"),
                    agent,
                    team,
                    ev.get("creationDate")
                ))

            execute_values(
                cur,
                """
                INSERT INTO visualizacao_resolucao.resolucao_por_status
                  (ticket_id, protocol, status, justificativa, seconds_utl,
                   permanency_time_fulltime_seconds, agent_name, team_name, changed_date)
                VALUES %s
                """,
                rows
            )
    conn.close()

def main():
    history = fetch_status_history(TICKET_ID)
    save_to_db(history)
    print(f"✅ Importados {len(history)} eventos do ticket {TICKET_ID}")

if __name__ == "__main__":
    main()
