import os
import time
import requests
import psycopg2
import psycopg2.extras
import datetime
import logging

# === CONFIGURAÇÕES ===
API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
LIMIT = int(os.getenv("PAGES_UPSERT", "10"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] detail: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# === REQUISIÇÕES COM RE-TENTATIVAS ===
def req(url, params=None, timeout=90):
    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            logging.warning(f"Rate limit, aguardando {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code in (404, 400):
            return None
        r.raise_for_status()
        return r.json() if r.text else None

# === CONSULTA MISSING ===
def fetch_pending_from_audit(conn, limit):
    sql = """
        select distinct ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = 'tickets_resolvidos'
        order by ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = [r[0] for r in cur.fetchall()]
    return rows

# === CONSULTA API ===
def fetch_ticket_with_fallback(ticket_id):
    """
    Tenta primeiro /tickets?id=...
    Se der 404/400, tenta /tickets/past?id=...
    """
    common_select = (
        "id,subject,type,status,baseStatus,ownerTeam,serviceFirstLevel,"
        "serviceSecondLevel,serviceThirdLevel,origin,category,urgency,"
        "createdDate,lastUpdate,resolvedIn,closedIn,cancelledIn,owner,clients"
    )
    expand = "createdBy,owner,clients($expand=organization)"
    base_params = {
        "token": TOKEN,
        "$select": common_select,
        "$expand": expand,
        "$top": 1,
        "$filter": f"id eq {ticket_id}"
    }

    # Tentativa 1: /tickets
    url_main = f"{API_BASE}/tickets"
    r = req(url_main, base_params)
    if r and isinstance(r, list) and len(r) > 0:
        return r[0], "tickets"

    # Tentativa 2: /tickets/past
    url_past = f"{API_BASE}/tickets/past"
    r = req(url_past, base_params)
    if r and isinstance(r, list) and len(r) > 0:
        return r[0], "tickets/past"

    # Tentativa 3: /tickets/merged (fallback final)
    url_merged = f"{API_BASE}/tickets/merged"
    merged_params = {"token": TOKEN, "$filter": f"id eq {ticket_id}", "$top": 1}
    r = req(url_merged, merged_params)
    if r and isinstance(r, list) and len(r) > 0:
        return r[0], "tickets/merged"

    return None, None

# === TRANSFORMAÇÃO ===
def normalize_ticket(t, endpoint):
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if clients else {}
    org = c0.get("organization") or {}

    last_resolved = t.get("resolvedIn") or None
    last_closed = t.get("closedIn") or None
    last_cancelled = t.get("cancelledIn") or None

    return {
        "ticket_id": int(t.get("id")),
        "owner_name": owner.get("businessName") or owner.get("name"),
        "owner_team_name": t.get("ownerTeam"),
        "origin": t.get("origin"),
        "status": t.get("status"),
        "last_resolved_at": last_resolved,
        "last_closed_at": last_closed,
        "last_cancelled_at": last_cancelled,
        "raw_payload": t,
        "imported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "endpoint": endpoint,
    }

# === INSERÇÃO GENÉRICA ===
def upsert_in_table(conn, row, table):
    sql = f"""
        insert into visualizacao_resolvidos.{table}
        (ticket_id, owner_name, owner_team_name, origin, status,
         last_resolved_at, last_closed_at, last_cancelled_at,
         raw_payload, imported_at)
        values (%(ticket_id)s, %(owner_name)s, %(owner_team_name)s, %(origin)s, %(status)s,
                %(last_resolved_at)s, %(last_closed_at)s, %(last_cancelled_at)s,
                %(raw_payload)s, %(imported_at)s)
        on conflict (ticket_id) do update set
          owner_name = excluded.owner_name,
          owner_team_name = excluded.owner_team_name,
          origin = excluded.origin,
          status = excluded.status,
          last_resolved_at = excluded.last_resolved_at,
          last_closed_at = excluded.last_closed_at,
          last_cancelled_at = excluded.last_cancelled_at,
          raw_payload = excluded.raw_payload,
          imported_at = excluded.imported_at
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()

# === PROCESSAMENTO ===
def main():
    if not TOKEN or not DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    with psycopg2.connect(DSN) as conn:
        pending = fetch_pending_from_audit(conn, LIMIT)
        logging.info(f"{len(pending)} tickets pendentes para atualização (limite={LIMIT}).")

        for idx, ticket_id in enumerate(pending, 1):
            logging.info(f"Processando ticket {idx}/{len(pending)} (ID={ticket_id})")
            try:
                data, endpoint = fetch_ticket_with_fallback(ticket_id)
                if not data:
                    logging.warning(f"Ticket {ticket_id} não encontrado em nenhum endpoint.")
                    continue

                row = normalize_ticket(data, endpoint)

                # Decide a tabela de destino
                if endpoint == "tickets/merged":
                    upsert_in_table(conn, row, "tickets_mesclados")
                else:
                    upsert_in_table(conn, row, "tickets_resolvidos")

                logging.info(f"Ticket {ticket_id} atualizado com sucesso ({endpoint}).")

            except Exception as e:
                logging.error(f"Erro ao processar ticket_id={ticket_id}: {e}")
            time.sleep(THROTTLE)

        logging.info("Processamento concluído.")

# === EXECUÇÃO ===
if __name__ == "__main__":
    main()
