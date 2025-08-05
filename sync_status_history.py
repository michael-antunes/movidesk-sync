import os
import json
import requests
import psycopg2

def get_ticket_ids(conn):
    single = os.getenv('TICKET_ID')
    if single:
        return [int(single)]
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT ticket_id FROM visualizacao_resolucao.resolucao_por_status")
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids

def fetch_status_history(ticket_id):
    token = os.environ["MOVIDESK_TOKEN"]
    url = f"https://api.movidesk.com/public/v1/tickets/statusHistory?ticketId={ticket_id}&token={token}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def save_history(conn, events):
    cur = conn.cursor()
    for ev in events:
        cur.execute(
            """
            INSERT INTO visualizacao_resolucao.resolucao_por_status
              (ticket_id, protocol, status, justificativa, seconds_utl,
               permanency_time_fulltime_seconds, changed_by, changed_date,
               agent_name, team_name)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticket_id,status,justificativa) DO NOTHING
            """,
            (
                ev.get("ticketId"),
                ev.get("protocol"),
                ev.get("status"),
                ev.get("justificativa") or None,
                ev.get("secondsUtil") or 0,
                ev.get("permanencyTimeFulltimeSeconds") or 0,
                json.dumps(ev.get("changedBy")) if ev.get("changedBy") else None,
                ev.get("changedDate"),
                ev.get("agentName"),
                ev.get("teamName"),
            )
        )
    conn.commit()
    cur.close()

def main():
    dsn = os.environ["NEON_DSN"]
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("TRUNCATE visualizacao_resolucao.resolucao_por_status")
    conn.commit()
    cur.close()
    for tid in get_ticket_ids(conn):
        events = fetch_status_history(tid)
        save_history(conn, events)
    conn.close()

if __name__ == "__main__":
    main()
