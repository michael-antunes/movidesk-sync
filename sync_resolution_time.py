import os
import requests
import psycopg2
from datetime import date, datetime

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
start_date = date.today().isoformat()

def fetch_resolution_times():
    """
    Busca tickets marcados como Resolved ou Closed,
    trazendo id, protocolo, data de resolução e tempos.
    Trata retorno que pode ser dict{"items": [...]} ou diretamente uma lista.
    """
    params = {
        "token": API_TOKEN,
        "$select": "id,protocol,resolvedIn,lifetimeWorkingTime,stoppedTimeWorkingTime",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {start_date}"
    }
    url = "https://api.movidesk.com/public/v1/tickets"
    r = requests.get(url, params=params)
    r.raise_for_status()
    payload = r.json()

    # Se veio { "items": [...] }, retorna items, senão, assume que payload já é lista
    if isinstance(payload, dict):
        return payload.get("items", [])
    elif isinstance(payload, list):
        return payload
    else:
        return []

def save_to_db(records):
    """
    Insere ou atualiza cada registro na tabela visualizacao_resolucao.movidesk_resolution
    """
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")

    for t in records:
        ticket_id = t.get("id")
        protocol  = t.get("protocol")
        resolved_in_str = t.get("resolvedIn")
        # converter ISO string para datetime
        resolved_in = datetime.fromisoformat(resolved_in_str)

        lifetime = t.get("lifetimeWorkingTime") or 0
        stopped  = t.get("stoppedTimeWorkingTime") or 0
        # horas úteis = (lifetime – stopped) / 3600
        net_hours = (lifetime - stopped) / 3600 if lifetime is not None else None

        cur.execute("""
            INSERT INTO movidesk_resolution
              (ticket_id, protocol, resolved_in,
               lifetime_working_time, stopped_time_working_time, net_hours)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id, resolved_in) DO UPDATE
              SET protocol       = EXCLUDED.protocol,
                  lifetime_working_time     = EXCLUDED.lifetime_working_time,
                  stopped_time_working_time = EXCLUDED.stopped_time_working_time,
                  net_hours       = EXCLUDED.net_hours,
                  imported_at     = NOW();
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
