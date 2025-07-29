import os
import requests
import psycopg2

# Pegar o token e a string de conexão do Neon das variáveis de ambiente
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN", "").strip()

def fetch_tickets():
    """
    Busca todos os tickets com status Em atendimento, Aguardando ou Novo,
    trazendo também o responsável (owner).
    """
    url = "https://api.movidesk.com/public/v1/tickets"
    all_tickets = []
    skip = 0
    top  = 1000

    # Campos que vamos selecionar
    select_fields = [
        "id",
        "protocol",
        "type",
        "subject",
        "status",
        "baseStatus",
        "ownerTeam",
        "serviceFirstLevel",
        "serviceSecondLevel",
        "serviceThirdLevel",
        "createdDate",
        "lastUpdate"
    ]

    while True:
        params = {
            "token": API_TOKEN,
            "$select": ",".join(select_fields),
            # Expande o owner para trazer apenas o businessName
            "$expand": "owner($select=businessName)",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        all_tickets.extend(batch)
        if len(batch) < top:
            break

        skip += len(batch)

    return all_tickets

def upsert_tickets(conn, tickets):
    """
    Insere ou atualiza os tickets na tabela movidesk_tickets_abertos,
    incluindo o nome do responsável.
    """
    sql = """
    INSERT INTO visualizacao_atual.movidesk_tickets_abertos
      (id, protocol, type, subject, status, base_status, owner_team,
       service_first_level, service_second_level, service_third_level,
       created_date, last_update, responsavel)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE SET
      protocol            = EXCLUDED.protocol,
      type                = EXCLUDED.type,
      subject             = EXCLUDED.subject,
      status              = EXCLUDED.status,
      base_status         = EXCLUDED.base_status,
      owner_team          = EXCLUDED.owner_team,
      service_first_level = EXCLUDED.service_first_level,
      service_second_level= EXCLUDED.service_second_level,
      service_third_level = EXCLUDED.service_third_level,
      created_date        = EXCLUDED.created_date,
      last_update         = EXCLUDED.last_update,
      responsavel         = EXCLUDED.responsavel;
    """
    with conn.cursor() as cur:
        for t in tickets:
            # Extrai o nome do responsável (businessName) do objeto owner
            owner = t.get("owner", {})
            nome_responsavel = owner.get("businessName") if isinstance(owner, dict) else None

            cur.execute(sql, (
                t["id"],
                t.get("protocol"),
                t["type"],
                t["subject"],
                t["status"],
                t.get("baseStatus"),
                t.get("ownerTeam"),
                t.get("serviceFirstLevel"),
                t.get("serviceSecondLevel"),
                t.get("serviceThirdLevel"),
                t["createdDate"],
                t["lastUpdate"],
                nome_responsavel
            ))
    conn.commit()

def main():
    print("🔄 Starting sync…")
    tickets = fetch_tickets()
    print(f"🔎 {len(tickets)} tickets fetched.")
    conn = psycopg2.connect(DSN)
    upsert_tickets(conn, tickets)
    conn.close()
    print("✅ Sync complete.")

if __name__ == "__main__":
    main()
