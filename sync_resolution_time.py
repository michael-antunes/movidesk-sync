import os
import requests
import psycopg2
from datetime import date

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
start_date = date.today().isoformat()

def fetch_resolved():
    params = {
        "token": API_TOKEN,
        "$select": "id,protocol,resolvedIn,lifetimeWorkingTime,stoppedTimeWorkingTime",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {start_date}"
    }
    r = requests.get("https://api.movidesk.com/public/v1/tickets", params=params)
    r.raise_for_status()
    return r.json()

def save_to_db(records):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movidesk_resolution (
            ticket_id INT,
            protocol TEXT,
            resolved_in TIMESTAMP,
            net_hours NUMERIC,
            imported_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (ticket_id, resolved_in)
        )
    """)
    for t in records:
        net = (t["lifetimeWorkingTime"] - t["stoppedTimeWorkingTime"]
               if t["lifetimeWorkingTime"] is not None and t["stoppedTimeWorkingTime"] is not None
               else None)
        hours = round(net/60, 2) if net is not None else None
        cur.execute("""
            INSERT INTO movidesk_resolution(ticket_id, protocol, resolved_in, net_hours)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticket_id, resolved_in) DO UPDATE
            SET net_hours = EXCLUDED.net_hours, imported_at = NOW()
        """, (t["id"], t["protocol"], t["resolvedIn"], hours))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    tickets = fetch_resolved()
    save_to_db(tickets)
