#!/usr/bin/env python3
import os
import re
import time
import json
import requests
import psycopg2
from datetime import date, datetime

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
START_DATE = date.today().isoformat()

if not API_TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN e NEON_DSN devem estar definidos")

BASE_URL = "https://api.movidesk.com/public/v1"
HEADERS  = {"token": API_TOKEN}

_user_team_cache = {}

def parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    # trunca fração de segundos além de 6 dígitos
    s_fixed = re.sub(r'\.(\d{6})\d+', r'.\1', s)
    return datetime.fromisoformat(s_fixed)

def fetch_resolved_ticket_ids() -> list[int]:
    """Busca IDs de tickets Resolved ou Closed desde START_DATE."""
    params = {
        "token": API_TOKEN,
        "$select": "id",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {START_DATE}",
        "$top": 500
    }
    resp = requests.get(f"{BASE_URL}/tickets", params=params)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items") if isinstance(data, dict) else data
    return [t["id"] for t in items]

def fetch_status_history(ticket_id: int) -> list[dict]:
    """Puxa o histórico de status de um ticket."""
    resp = requests.get(
        f"{BASE_URL}/tickets/statusHistory",
        headers=HEADERS,
        params={"ticketId": ticket_id}
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("items") if isinstance(data, dict) else data

def get_user_team(user_id: int) -> str | None:
    """Busca o nome da equipe de um usuário, com cache."""
    if user_id in _user_team_cache:
        return _user_team_cache[user_id]
    resp = requests.get(f"{BASE_URL}/users/{user_id}", headers=HEADERS)
    if resp.status_code != 200:
        return None
    team = resp.json().get("team", {}).get("name")
    _user_team_cache[user_id] = team
    return team

def save_history(records: list[dict]):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")
    for e in records:
        tid  = e.get("ticketId")
        proto = e.get("protocol")
        status = e.get("status")
        justif = e.get("justification") or "-"
        # permanencyTimeFullTime = segundos corridos
        seconds_utl = e.get("permanencyTimeFullTime")
        # permanencyTimeWorkingTime = segundos úteis
        seconds_utl_work = e.get("permanencyTimeWorkingTime")
        cb = e.get("changedBy") or {}
        changed_by = json.dumps(cb)
        changed_date = parse_iso(e.get("changedDate"))
        agent_name = cb.get("name")
        team_name  = get_user_team(cb.get("id")) if cb.get("id") else None

        cur.execute("""
        INSERT INTO resolucao_por_status (
          ticket_id, protocol, status, justificativa,
          seconds_utl, permanency_time_fulltime_seconds,
          agent_name, team_name,
          changed_by, changed_date, imported_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (ticket_id,status,justificativa) DO UPDATE
          SET seconds_utl                      = EXCLUDED.seconds_utl,
              permanency_time_fulltime_seconds = EXCLUDED.permanency_time_fulltime_seconds,
              agent_name                       = EXCLUDED.agent_name,
              team_name                        = EXCLUDED.team_name,
              changed_by                       = EXCLUDED.changed_by,
              changed_date                     = EXCLUDED.changed_date,
              imported_at                      = NOW()
        """, (
          tid, proto, status, justif,
          seconds_utl, seconds_utl_work,
          agent_name, team_name,
          changed_by, changed_date
        ))
    conn.commit()
    cur.close()
    conn.close()

def main():
    print("=== Iniciando sync_status_history.py ===")
    ticket_ids = fetch_resolved_ticket_ids()
    print(f"→ {len(ticket_ids)} tickets a processar")
    all_events = []
    for tid in ticket_ids:
        evs = fetch_status_history(tid)
        print(f"  Ticket {tid}: {len(evs)} eventos")
        all_events.extend(evs)
        time.sleep(0.2)
    print(f"→ Gravando {len(all_events)} eventos no banco")
    save_history(all_events)
    print("=== Concluído com sucesso ===")

if __name__ == "__main__":
    main()
