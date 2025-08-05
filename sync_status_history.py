import os
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL = "https://api.movidesk.com/public/v1/tickets/past"
TOKEN = os.environ["MOVIDESK_TOKEN"]
TICKET_ID = os.environ.get("TICKET_ID")

if not TICKET_ID:
    raise SystemExit("❌ Variável TICKET_ID não definida")

def fetch_status_history(ticket_id):
    params = {
        "token": TOKEN,
        "id": ticket_id,
        "$expand": "statusHistories($expand=changedBy($select=businessName),changedByTeam($select=businessName))",
    }
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    data = r.json()
    return data.get("statusHistories", [])

def save_to_db(records):
    conn = psycopg2.connect(os.environ["NEON_DSN"])
    with conn:
        with conn.cursor() as cur:
            # limpa só as linhas do ticket atual
            cur.execute(
                "DELETE FROM visualizacao_resolucao.resolucao_por_status WHERE ticket_id = %s;",
                (TICKET_ID,),
            )
            if records:
                rows = []
                for ev in records:
                    rows.append((
                        int(TICKET_ID),
                        ev.get("ticketId"),
                        ev.get("status"),
                        ev.get("justification") or "",
                        ev.get("secondsUntilNextStatus"),
                        ev.get("permanencyTimeFullTimeSeconds"),
                        ev.get("changedBy", {}).get("businessName"),
                        ev.get("changedByTeam", {}).get("businessName"),
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
    hist = fetch_status_history(TICKET_ID)
    save_to_db(hist)
    print(f"✅ Histórico de status do ticket {TICKET_ID} importado com {len(hist)} registros")

if __name__ == "__main__":
    main()
