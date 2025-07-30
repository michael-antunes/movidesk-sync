import os
import json
import requests
import psycopg2
from datetime import datetime, timezone

API_URL           = "https://api.movidesk.com/public/v1"
MOVI_TOKEN        = os.environ["MOVIDESK_TOKEN"]
NEON_DSN          = os.environ["NEON_DSN"]
SCHEMA            = "visualizacao_resolucao"
TABLE             = f"{SCHEMA}.resolucao_por_status"
HEADERS           = {"Content-Type": "application/json", "token": MOVI_TOKEN}

def get_ticket_ids():
    sql = f"""
    SELECT DISTINCT ticket_id
      FROM {SCHEMA}.resolucao_por_status
    UNION
    SELECT DISTINCT ticketId
      FROM tickets_movidesk
     WHERE statusBase = 'Resolvido'
    """
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]

def fetch_status_history(ticket_id):
    url = f"{API_URL}/tickets/statusHistory?ticketId={ticket_id}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()  # retorna lista de eventos

def save_to_db(ticket_id, protocol, history):
    if not history:
        return
    rows = []
    for ev in history:
        status      = ev.get("status")
        justification = ev.get("justification") or "-"
        seconds_utl = ev.get("secondsUtl", 0)
        permanency  = ev.get("permanencyTimeFulltimeSeconds")
        changed_by  = json.dumps(ev.get("changedBy")) if ev.get("changedBy") else None
        changed_date= ev.get("changedDate")
        agent_name  = ev.get("agent",{}).get("name")
        team_name   = ev.get("team",{}).get("name")
        rows.append((
            ticket_id,
            protocol,
            status,
            justification,
            seconds_utl,
            permanency,
            changed_by,
            changed_date,
            agent_name,
            team_name
        ))

    insert_sql = f"""
    INSERT INTO {TABLE}
      (ticket_id, protocol, status, justificativa,
       seconds_utl, permanency_time_fulltime_seconds,
       changed_by, changed_date,
       agent_name, team_name, imported_at)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
    ON CONFLICT (ticket_id, status, justificativa) DO NOTHING;
    """

    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
        conn.commit()

def main():
    ids = get_ticket_ids()
    for tid in ids:
        history = fetch_status_history(tid)
        protocol = None
        # opcional: buscar protocol no ticket-seu-código se precisar
        save_to_db(tid, protocol, history)

if __name__ == "__main__":
    main()
