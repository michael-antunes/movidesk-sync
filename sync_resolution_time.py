import os
import requests
import psycopg2
from datetime import date

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
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
    cur.execute("SET search_path TO visualizacao_resolucao;")
    for t in records:
        lt = t.get("lifetimeWorkingTime")
        st = t.get("stoppedTimeWorkingTime")
        net = lt - st if lt is not None and st is not None else None
        hours = round(net/60, 2) if net is not None else None

        cur.execute(
            """
            INSERT INTO movidesk_resolution
              (ticket_id, protocol, resolved_in,
               lifetime_working_time, stopped_time_working_time,
               net_hours)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticket_id, resolved_in) DO UPDATE
              SET lifetime_working_time       = EXCLUDED.lifetime_working_time,
                  stopped_time_working_time   = EXCLUDED.stopped_time_working_time,
                  net_hours                   = EXCLUDED.net_hours,
                  imported_at                 = NOW()
            """,
            (
              t["id"],
              t.get("protocol"),
              t["resolvedIn"],
              lt,
              st,
              hours
            )
        )
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    tickets = fetch_resolved()
    save_to_db(tickets)
