import os
import requests
import psycopg2
from datetime import date, datetime

# token e DSN vindo dos secrets do GitHub Actions
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")

# vamos buscar tudo a partir de hoje (você pode ajustar essa data)
start_date = date.today().isoformat()

def fetch_resolution_times():
    """
    Busca tickets marcados como Resolved ou Closed,
    trazendo id, protocolo, data de resolução e tempos de vida/parado.
    """
    params = {
        "token": API_TOKEN,
        # seleciona os campos que precisamos
        "$select": "id,protocol,resolvedIn,lifetimeWorkingTime,stoppedTimeWorkingTime",
        # filtra só tickets resolvidos ou fechados desde start_date
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {start_date}"
    }
    url = "https://api.movidesk.com/public/v1/tickets"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    # Movidesk retorna uma lista em data["items"]
    return data.get("items", [])

def save_to_db(records):
    """
    Insere ou atualiza cada registro na tabela visualizacao_resolucao.movidesk_resolution
    """
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    # usar o schema criado
    cur.execute("SET search_path TO visualizacao_resolucao;")

    for t in records:
        ticket_id = t.get("id")
        protocol  = t.get("protocol")
        resolved_in_str = t.get("resolvedIn")
        # converter a string ISO para objeto datetime do Python
        resolved_in = datetime.fromisoformat(resolved_in_str)

        lifetime = t.get("lifetimeWorkingTime")           # em segundos
        stopped  = t.get("stoppedTimeWorkingTime", 0)     # em segundos
        net_hours = None
        if lifetime is not None:
            # horas úteis = (lifetime - stopped) / 3600
            net_hours = (lifetime - stopped) / 3600

        # upsert na tabela
        cur.execute("""
            INSERT INTO movidesk_resolution
              (ticket_id, protocol, resolved_in,
               lifetime_working_time, stopped_time_working_time, net_hours)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id, resolved_in) DO UPDATE
              SET protocol = EXCLUDED.protocol,
                  lifetime_working_time = EXCLUDED.lifetime_working_time,
                  stopped_time_working_time = EXCLUDED.stopped_time_working_time,
                  net_hours = EXCLUDED.net_hours,
                  imported_at = NOW();
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
