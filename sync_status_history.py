import os
import json
import time
import requests
import psycopg2
from datetime import date, datetime

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
START_DATE = date.today().isoformat()

BASE_URL = "https://api.movidesk.com/public/v1"
HEADERS = {"token": API_TOKEN}

_user_team_cache = {}

def parse_iso(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    parts = dt_str.split('.')
    if len(parts) == 2 and len(parts[1]) > 6:
        parts[1] = parts[1][:6]
        dt_str = '.'.join(parts)
    return datetime.fromisoformat(dt_str)

def fetch_resolved_ticket_ids() -> list[int]:
    today = date.today().isoformat()
    params = {
        "token": API_TOKEN,
        "$select": "id",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {today}",
        "$top": 500
    }
    resp = requests.get(f"{BASE_URL}/tickets", params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items") if isinstance(data, dict) else data
    return [t["id"] for t in items]

def fetch_status_history(ticket_id: int) -> list[dict]:
    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        "$expand": (
            "statusHistories("
            "$select=status,justification,"
            "permanencyTimeWorkingTime,permanencyTimeFullTime,"
            "changedBy,changedDate)"
        )
    }
    resp = requests.get(f"{BASE_URL}/tickets", params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items") if isinstance(data, dict) else data
    if items:
        return items[0].get("statusHistories", [])
    return []

def save_history(events: list[dict]):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")
    for e in events:
        tid = e.get("ticketId")
        status = e.get("status")
        justification = e.get("justification") or "-"
        sec_fulltime = e.get("permanencyTimeFullTime")
        sec_working = e.get("permanencyTimeWorkingTime")
        cb = e.get("changedBy") or {}
        changed_by = json.dumps(cb, ensure_ascii=False)
        changed_date = parse_iso(e.get("changedDate"))
        agent_name = cb.get("name")
        team_name = cb.get("team", {}).get("name")

        cur.execute(
            """
            INSERT INTO visualizacao_resolucao.resolucao_por_status
              (ticket_id, protocol, status, justificativa,
               seconds_utl, permanency_time_fulltime_seconds,
               agent_name, team_name,
               changed_by, changed_date, imported_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
              SET seconds_utl                      = EXCLUDED.seconds_utl,
                  permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
                  agent_name                       = EXCLUDED.agent_name,
                  team_name                        = EXCLUDED.team_name,
                  changed_by                       = EXCLUDED.changed_by,
                  changed_date                     = EXCLUDED.changed_date,
                  imported_at                      = NOW();
            """,
            (
                tid,
                None,
                status,
                justification,
                sec_fulltime,
                sec_working,
                agent_name,
                team_name,
                changed_by,
                changed_date
            )
        )
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("=== Iniciando sync_status_history.py ===")
    ticket_ids = fetch_resolved_ticket_ids()
    print(f"→ {len(ticket_ids)} tickets para processar")
    all_events = []
    for tid in ticket_ids:
        hist = fetch_status_history(tid)
        print(f"  Ticket {tid}: importando {len(hist)} eventos")
        for ev in hist:
            ev["ticketId"] = tid
        all_events.extend(hist)
        time.sleep(0.2)
    print(f"→ Gravando {len(all_events)} eventos no banco")
    save_history(all_events)
    print("=== Concluído com sucesso ===")

if __name__ == "__main__":
    main()
