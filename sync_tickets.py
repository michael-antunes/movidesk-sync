import os
import requests
import psycopg2
import time

API_TOKEN = os.getenv("API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

def fetch_tickets():
    """
    Puxa todos os tickets com status "Em atendimento" ou "Aguardando",
    paginando até não vir mais nada.
    """
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    tickets = []
    page = 1
    max_retries = 5

    while True:
        for attempt in range(1, max_retries+1):
            resp = requests.get(
                "https://api.movidesk.com/public/v1/tickets",
                headers=headers,
                params=[
                  ("status", "Em atendimento"),
                  ("status", "Aguardando"),
                  ("page", page),
                ],
            )
            if resp.status_code == 200:
                break
            else:
                wait = 2 ** (attempt - 1)
                print(f"⚠️  Server error {resp.status_code} on page {page}, attempt {attempt}/{max_retries}, sleeping {wait}s")
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch page {page} after {max_retries} retries")

        data = resp.json()
        if not data:
            # acabou a paginação
            break

        tickets.extend(data)
        page += 1

    print(f"✔️  Fetched {len(tickets)} tickets.")
    return tickets

def clear_table(conn):
    """Limpa toda a tabela antes de inserir o snapshot atual."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE visualizacao_atual.movidesk_tickets_abertos;")
    conn.commit()

def upsert_tickets(conn, tickets):
    """
    Insere (ou atualiza) todos os tickets no Neon.
    Usa ON CONFLICT(id) DO UPDATE para manter apenas o snapshot.
    """
    sql = """
    INSERT INTO visualizacao_atual.movidesk_tickets_abertos
      (id, protocol, type, subject, status, base_status, owner_team,
       service_first_level, created_date, last_update)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      protocol           = EXCLUDED.protocol,
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
              t.get("lastUpdate"),
            ))
    conn.commit()

def main():
    tickets = fetch_tickets()

    print("🔄 Conectando ao Neon…")
    conn = psycopg2.connect(NEON_DSN)
    print("✔️  Conectado.")

    print("🗑️  Limpando tabela no Neon…")
    clear_table(conn)

    if tickets:
        print(f"↗️  Inserindo {len(tickets)} registros…")
        upsert_tickets(conn, tickets)
    else:
        print("⚠️  Sem tickets para inserir.")

    conn.close()
    print("✅ Sync concluído.")

if __name__ == "__main__":
    main()
