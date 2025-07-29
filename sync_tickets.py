import os
import time
import requests
import psycopg2

API_TOKEN = os.getenv("API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

def fetch_tickets(status_list=("Em atendimento","Aguardando"), max_retries=5):
    """
    Faz paginação e tenta até max_retries em cada página.
    Se mesmo assim falhar, considera que não há tickets nessa página.
    """
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    tickets = []
    page = 1

    while True:
        params = {
            "status": ",".join(status_list),
            "page": page
        }
        for attempt in range(1, max_retries + 1):
            resp = requests.get("https://api.movidesk.com/public/v1/tickets", headers=headers, params=params)
            if resp.status_code == 200:
                data = resp.json()
                if not data:
                    return tickets
                tickets.extend(data)
                page += 1
                break
            else:
                wait = 2 ** attempt
                print(f"⚠️  Server error {resp.status_code} on page {page}, attempt {attempt}/{max_retries}, sleeping {wait}s")
                time.sleep(wait)
        else:
            # todas tentativas falharam, considera página vazia e encerra
            print(f"⚠️  FAILED to fetch page {page} after {max_retries} retries, ending pagination.")
            return tickets

    return tickets

def clear_table(conn):
    """Limpa toda a tabela antes de inserir o snapshot atual."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE visualizacao_atual.movidesk_tickets_abertos;")
    conn.commit()
    print("🗑  Table truncated.")

def upsert_tickets(conn, tickets):
    """
    Insere ou atualiza cada ticket via ON CONFLICT.
    """
    sql = """
    INSERT INTO visualizacao_atual.movidesk_tickets_abertos
      (id, protocol, type, subject, status, base_status, owner_team, service_first_level, created_date, last_update)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      protocol          = EXCLUDED.protocol,
      type              = EXCLUDED.type,
      subject           = EXCLUDED.subject,
      status            = EXCLUDED.status,
      base_status       = EXCLUDED.base_status,
      owner_team        = EXCLUDED.owner_team,
      service_first_level = EXCLUDED.service_first_level,
      created_date      = EXCLUDED.created_date,
      last_update       = EXCLUDED.last_update;
    """
    # psycopg2.extras.execute_values pode ajudar performance, mas aqui deixo manual
    with conn.cursor() as cur:
        for t in tickets:
            cur.execute(sql, [
                (
                  t.get("id"),
                  t.get("protocol"),
                  t.get("type"),
                  t.get("subject"),
                  t.get("status"),
                  t.get("baseStatus"),
                  t.get("ownerTeam"),
                  t.get("serviceFirstLevel"),
                  t.get("createdDate"),
                  t.get("lastUpdate"),
                )
            ])
    conn.commit()
    print(f"✅  Upserted {len(tickets)} tickets.")

def main():
    print("▶️  Starting sync...")
    tickets = fetch_tickets()
    print(f"✔️  Fetched {len(tickets)} tickets.")

    conn = psycopg2.connect(NEON_DSN)
    clear_table(conn)
    upsert_tickets(conn, tickets)
    conn.close()

if __name__ == "__main__":
    main()
