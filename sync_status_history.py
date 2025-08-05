import os
import json
import requests
import psycopg2
from datetime import datetime

TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
TICKET_ID = os.getenv("TICKET_ID")

def fetch_status_history(ticket_id):
    url = (
        f"https://api.movidesk.com/public/v1/tickets/past"
        f"?token={TOKEN}&id={ticket_id}"
        f"&$expand=statusHistories("
        f"$expand=changedBy($select=businessName),"
        f"changedByTeam($select=businessName))"
    )
    r = requests.get(url)
    r.raise_for_status()
    return r.json().get("statusHistories", [])

def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("DELETE FROM visualizacao_resolucao.resolucao_por_status;")
    for ev in fetch_status_history(TICKET_ID):
        status = ev.get("status")
        justificativa = ev.get("justification") or ""
        seconds_utl = ev.get("permanencyTimeWorkingTime")
        fulltime = ev.get("permanencyTimeFullTime")
        changed_date = datetime.fromisoformat(ev.get("changedDate"))
        agent = ev.get("changedBy", {}).get("businessName")
        team = ev.get("changedByTeam", {}).get("businessName")
        cur.execute(
            """
            INSERT INTO visualizacao_resolucao.resolucao_por_status
            (ticket_id, status, justificativa, seconds_utl,
             permanency_time_fulltime_seconds, changed_by,
             changed_date, agent_name, team_name)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                TICKET_ID, status, justificativa,
                seconds_utl, fulltime,
                json.dumps(ev.get("changedBy")), changed_date,
                agent, team
            )
        )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
