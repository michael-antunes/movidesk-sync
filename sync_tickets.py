# sync_tickets.py

import os
import requests
import psycopg2

API_TOKEN = os.getenv("API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

def fetch_tickets():
    """
    Puxa todos os tickets com status "Em atendimento" OU "Aguardando".
    Reparou que a API Movidesk aceita múltiplos parâmetros ?status= via lista de tuplas.
    """
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json"
    }
    tickets = []
    page = 1

    while True:
        # aqui passamos vários status de uma vez sem usar vírgula em string
        params = [
            ("status", "Em atendimento"),
            ("status", "Aguardando"),
            ("page", page)
        ]
        resp = requests.get(
            "https://api.movidesk.com/public/v1/tickets",
            headers=headers,
            params=params
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        tickets.extend(data)
        page += 1

    return tickets

def clear_table(conn):
    """Limpa a tabela antes de inserir o snapshot atual."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE visualizacao_atual.movidesk_tickets_abertos;")
    conn.commit()

def upsert_tickets(conn, tickets):
    """Insere ou atualiza cada ticket no Neon."""
    sql = """
    INSERT INTO visualizacao_atual.movidesk_tickets_abertos
      (id, protocol, type, subject, status, base_status,
       owner_team, service_first_level, created_date, last_update)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE
    SET protocol           = EXCLUDED.protocol,
        type               = EXCLUDED.type,
        subject            = EXCLUDED.subject,
        status             = EXCLUDED.status,
        base_status        = EXCLUDED.base_status,
        owner_team         = EXCLUDED.owner_team,
        service_first_level= EXCLUDED.service_first_level,
        created_date       = EXCLUDED.created_date,
        last_update        = EXCLUDED.last_update;
    """
    with conn.cursor() as cur:
        for t in tickets:
            cur.execute(sql, (
                t["id"],
                t.get("protocol"),
                t.get("type"),
                t.get("subject"),
                t.get("status"),
                t.get("baseStatus"),
                t.get("ownerTeam"),
                t.get("serviceFirstLevel"),
                t.get("createdDate"),
                t.get("lastUpdate")
            ))
    conn.commit()

def main():
    tickets = fetch_tickets()
    conn = psycopg2.connect(NEON_DSN)
    clear_table(conn)
    upsert_tickets(conn, tickets)
    conn.close()
    print(f"✓ Sincronizados {len(tickets)} tickets")

if __name__ == "__main__":
    main()
