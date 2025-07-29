# sync_tickets.py
import os
import requests
import psycopg2

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")

def fetch_tickets():
    """Puxa todos os tickets ‘Em atendimento’ ou ‘Aguardando’ da Movidesk."""
    url         = "https://api.movidesk.com/public/v1/tickets"
    all_tickets = []
    skip        = 0
    top         = 100

    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,createdDate,lastUpdate",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando')",
            "$top": top,
            "$skip": skip
        }
        resp = requests.get(url, params=params)
        print(f"🔄  DEBUG fetch: GET {resp.url} → {resp.status_code}")
        resp.raise_for_status()

        batch = resp.json()
        if not batch:
            break

        all_tickets.extend(batch)
        skip += len(batch)
        if len(batch) < top:
            break

    return all_tickets

def upsert_tickets(conn, tickets):
    """Insere/atualiza cada ticket no Neon."""
    with conn.cursor() as cur:
        for t in tickets:
            cur.execute("""
                INSERT INTO visualizacao_atual.movidesk_tickets_abertos
                  (id, protocol, type, subject, status, base_status, owner_team, service_first_level, created_date, last_update)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                  protocol            = EXCLUDED.protocol,
                  type                = EXCLUDED.type,
                  subject             = EXCLUDED.subject,
                  status              = EXCLUDED.status,
                  base_status         = EXCLUDED.base_status,
                  owner_team          = EXCLUDED.owner_team,
                  service_first_level = EXCLUDED.service_first_level,
                  created_date        = EXCLUDED.created_date,
                  last_update         = EXCLUDED.last_update;
            """, (
                t["id"], t.get("protocol"), t["type"], t["subject"],
                t["status"], t.get("baseStatus"), t.get("ownerTeam"),
                t.get("serviceFirstLevel"), t["createdDate"], t["lastUpdate"]
            ))
        conn.commit()

def main():
    print("🔄  Starting sync…")
    tickets = fetch_tickets()
    print(f"⚠️  DEBUG: trouxe {len(tickets)} tickets")
    if not tickets:
        print("🚫  Sem tickets, saindo.")
        return

    print("🔒  Conectando ao Neon…")
    conn = psycopg2.connect(DSN)
    upsert_tickets(conn, tickets)
    conn.close()
    print("✅  Sync concluído.")

if __name__ == "__main__":
    main()
