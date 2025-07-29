import os, requests, psycopg2, json
from datetime import date

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
start_date = date.today().isoformat()

def fetch_history():
    params = {
        "token": API_TOKEN,
        "$select": "id,protocol,resolvedIn",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {start_date}",
        "$expand": (
            "statusHistories("
            "$select=status,justification,permanencyTimeWorkingTime,"
            "permanencyTimeFullTime,changedBy,changedDate)"
        )
    }
    r = requests.get("https://api.movidesk.com/public/v1/tickets", params=params)
    r.raise_for_status()
    # Aqui r.json() já é uma lista de tickets
    return r.json()

def save_history(records):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")
    for t in records:
        ticket_id = t["id"]
        protocol  = t.get("protocol")
        for h in t.get("statusHistories", []):
            cur.execute(
                """
                INSERT INTO resolucao_por_status
                  (ticket_id, protocol, status, justificativa,
                   seconds_utl, permanency_time_fulltime_seconds,
                   changed_by, changed_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ticket_id,status,justificativa) DO UPDATE
                  SET seconds_utl                      = EXCLUDED.seconds_utl,
                      permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
                      changed_by                       = EXCLUDED.changed_by,
                      changed_date                     = EXCLUDED.changed_date,
                      imported_at                      = NOW()
                """,
                (
                  ticket_id,
                  protocol,
                  h.get("status"),
                  h.get("justification"),
                  h.get("permanencyTimeWorkingTime"),
                  h.get("permanencyTimeFullTime"),
                  json.dumps(h.get("changedBy")),
                  h.get("changedDate")
                )
            )
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    tickets_h = fetch_history()
    save_history(tickets_h)
