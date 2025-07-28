import requests
import psycopg2

API_TOKEN = "aa40560c-496c-4fa6-b18c-88446839544a"

dsn = (
    "postgresql://tickets-movidesk_owner:"
    "npg_ymUNzQcM80Vj"
    "@ep-fragrant-art-a45v8csv-pooler.us-east-1.aws.neon.tech/"
    "tickets-movidesk?sslmode=require&channel_binding=require"
)

def fetch_tickets():
    url = "https://api.movidesk.com/public/v1/tickets"
    all_tickets = []
    skip = 0
    top  = 100

    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,createdDate,lastUpdate",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        all_tickets.extend(batch)
        skip += len(batch)
        if len(batch) < top:
            break

    return all_tickets

def upsert_tickets(conn, tickets):
    with conn.cursor() as cur:
        for t in tickets:
            cur.execute("""
                INSERT INTO visualizacao_atual.movidesk_tickets_abertos
                  (id, protocol, type, subject, status, base_status, owner_team, service_first_level, created_date, last_update)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                  protocol=EXCLUDED.protocol,
                  type=EXCLUDED.type,
                  subject=EXCLUDED.subject,
                  status=EXCLUDED.status,
                  base_status=EXCLUDED.base_status,
                  owner_team=EXCLUDED.owner_team,
                  service_first_level=EXCLUDED.service_first_level,
                  created_date=EXCLUDED.created_date,
                  last_update=EXCLUDED.last_update;
            """, (
                t["id"], t.get("protocol"), t["type"], t["subject"],
                t["status"], t.get("baseStatus"), t.get("ownerTeam"),
                t.get("serviceFirstLevel"), t["createdDate"], t["lastUpdate"]
            ))
        conn.commit()

def main():
    tickets = fetch_tickets()
    print(f"🔎 Inserindo {len(tickets)} tickets com status Em atendimento/Aguardando...")
    conn = psycopg2.connect(dsn)
    upsert_tickets(conn, tickets)
    conn.close()

if __name__ == "__main__":
    main()
