import os
import re
import requests
import psycopg2
from datetime import date, datetime

API_TOKEN  = os.getenv("MOVIDESK_TOKEN")
DSN        = os.getenv("NEON_DSN")
start_date = date.today().isoformat()

def fetch_resolution_times():
    params = {
        "token": API_TOKEN,
        "$select": "id,protocol,resolvedIn,lifetimeWorkingTime,stoppedTimeWorkingTime",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {start_date}"
    }
    r = requests.get("https://api.movidesk.com/public/v1/tickets", params=params)
    r.raise_for_status()
    payload = r.json()
    # a API às vezes retorna { "items": [...] } ou retorna diretamente uma lista
    if isinstance(payload, dict):
        return payload.get("items", [])
    elif isinstance(payload, list):
        return payload
    else:
        return []

def parse_iso(s: str) -> datetime:
    # Trunca fração de segundo para até 6 dígitos (compatível com fromisoformat)
    s_fixed = re.sub(r'\.(\d{6})\d+', r'.\1', s)
    return datetime.fromisoformat(s_fixed)

def save_to_db(records):
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")

    for t in records:
        ticket_id = t.get("id")
        protocol  = t.get("protocol")
        rs_str    = t.get("resolvedIn")
        try:
            resolved_in = parse_iso(rs_str)
        except Exception:
            continue

        # lifetimeWorkingTime e stoppedTimeWorkingTime vêm em MINUTOS
        lifetime = t.get("lifetimeWorkingTime") or 0
        stopped  = t.get("stoppedTimeWorkingTime") or 0

        # calcula horas úteis: (minutos ativos) / 60, mas nunca deixa ficar negativo
        net_hours = max((lifetime - stopped) / 60, 0)

        cur.execute("""
          INSERT INTO movidesk_resolution
            (ticket_id, protocol, resolved_in,
             lifetime_working_time, stopped_time_working_time, net_hours)
          VALUES (%s, %s, %s, %s, %s, %s)
          ON CONFLICT (ticket_id, resolved_in) DO UPDATE
            SET protocol                   = EXCLUDED.protocol,
                lifetime_working_time      = EXCLUDED.lifetime_working_time,
                stopped_time_working_time  = EXCLUDED.stopped_time_working_time,
                net_hours                  = EXCLUDED.net_hours,
                imported_at                = NOW();
        """, (
          ticket_id,
          protocol,
          resolved_in,
          lifetime,
          stopped,
          net_hours
        ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    tickets = fetch_resolution_times()
    save_to_db(tickets)
