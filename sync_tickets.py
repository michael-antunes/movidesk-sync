import os
import requests
import psycopg2

API_TOKEN = os.getenv("API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

def fetch_tickets():
    # ... seu código de paginação/fetch ...
    return tickets

def clear_table(conn):
    """Limpa toda a tabela antes de inserir o snapshot atual."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE visualizacao_atual.movidesk_tickets_abertos;")
    conn.commit()

def upsert_tickets(conn, tickets):
    sql = """
    INSERT INTO visualizacao_atual.movidesk_tickets_abertos
      (id, protocol, type, subject, status, base_status, owner_team,
       service_first_level, created_date, last_update)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE
      SET protocol            = EXCLUDED.protocol,
          type                = EXCLUDED.type,
          subject             = EXCLUDED.subject,
          status              = EXCLUDED.status,
          base_status         = EXCLUDED.base_status,
          owner_team          = EXCLUDED.owner_team,
          service_first_level = EXCLUDED.service_first_level,
          created_date        = EXCLUDED.created_date,
          last_update         = EXCLUDED.last_update
    """

    with conn.cursor() as cur:
        for t in tickets:
            cur.execute(sql, (
                t["id"],
                t.get("protocol"),
                t["type"],
                t["subject"],
                t["status"],
                t.get("baseStatus"),
                t.get("ownerTeam"),
                t.get("serviceFirstLevel"),
                t["createdDate"],
                t["lastUpdate"],
            ))
    conn.commit()

def main():
    tickets = fetch_tickets()
    print(f"🔎 Total de tickets retornados pela API: {len(tickets)}")

    conn = psycopg2.connect(NEON_DSN)
    clear_table(conn)               # <-- limpa tudo
    upsert_tickets(conn, tickets)   # <-- insere só o snapshot atual
    conn.close()

if __name__ == "__main__":
    main()
