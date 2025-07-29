import requests
import psycopg2
import os

MOVIEDESK_TOKEN = os.getenv('MOVIEDESK_TOKEN') or 'SEU_TOKEN_AQUI'  # Troque aqui

DATABASE_URL = os.getenv('DATABASE_URL') or 'postgresql://tickets-movidesk_owner:SUA_SENHA@ep-fragrant-art-a45v8csv-pooler.us-east-1.aws.neon.tech/tickets-movidesk?sslmode=require'

def fetch_tickets():
    url = "https://api.movidesk.com/public/v1/tickets"
    params = {
        "token": MOVIEDESK_TOKEN,
        "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,createdDate,lastUpdate,serviceSecondLevel,serviceThirdLevel,responsavel",
        "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
        "$top": 1000,
        "$skip": 0,
    }
    tickets = []
    while True:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        tickets.extend(data)
        params["$skip"] += len(data)
        if len(data) < params["$top"]:
            break
    return tickets

def upsert_tickets(conn, tickets):
    sql = """
        INSERT INTO visualizacao_atual.movidesk_tickets_abertos (
            id, protocol, type, subject, status, base_status, owner_team,
            service_first_level, created_date, last_update,
            service_second_level, service_third_level, responsavel
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            protocol = EXCLUDED.protocol,
            type = EXCLUDED.type,
            subject = EXCLUDED.subject,
            status = EXCLUDED.status,
            base_status = EXCLUDED.base_status,
            owner_team = EXCLUDED.owner_team,
            service_first_level = EXCLUDED.service_first_level,
            created_date = EXCLUDED.created_date,
            last_update = EXCLUDED.last_update,
            service_second_level = EXCLUDED.service_second_level,
            service_third_level = EXCLUDED.service_third_level,
            responsavel = EXCLUDED.responsavel;
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
                t.get("lastUpdate"),
                t.get("serviceSecondLevel"),
                t.get("serviceThirdLevel"),
                t.get("responsavel")    # <-- Aqui pega o campo responsavel
            ))
    conn.commit()

def main():
    print("🔄 Starting sync…")
    tickets = fetch_tickets()
    print(f"🔎 {len(tickets)} tickets fetched.")
    conn = psycopg2.connect(DATABASE_URL)
    upsert_tickets(conn, tickets)
    conn.close()
    print("✅ Sync finished!")

if __name__ == "__main__":
    main()
