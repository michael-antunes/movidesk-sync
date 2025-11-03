import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

# Status que consideramos "abertos"
OPEN_STATUSES = ["Em atendimento", "Aguardando", "Novo"]

session = requests.Session()
session.headers.update({"Accept": "application/json"})


def fetch_open_ids():
    """Lista todos os IDs de tickets abertos, paginação por skip/top."""
    url = "https://api.movidesk.com/public/v1/tickets"
    ids, skip, top = [], 0, 1000
    movi_filter = "(" + " or ".join([f"status eq '{s}'" for s in OPEN_STATUSES]) + ")"

    while True:
        r = session.get(
            url,
            params={
                "token": API_TOKEN,
                "$select": "id",
                "$filter": movi_filter,
                "$top": top,
                "$skip": skip,
            },
            timeout=90,
        )
        r.raise_for_status()
        page = r.json() or []
        if not page:
            break
        for t in page:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            if tid is not None:
                ids.append(tid)
        if len(page) < top:
            break
        skip += len(page)

    return ids


def fetch_ticket_by_id(ticket_id):
    """
    Chama como seu DEV: /tickets?id={id} (sem $select/$expand).
    Assim garantimos que, quando houver, vem clients[0].organization.
    """
    url = "https://api.movidesk.com/public/v1/tickets"
    r = session.get(url, params={"token": API_TOKEN, "id": ticket_id}, timeout=90)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    arr = r.json() or []
    if isinstance(arr, list) and arr:
        return arr[0]
    return None


def coerce_int(v):
    try:
        return int(v) if str(v).isdigit() else None
    except Exception:
        return None


def map_ticket_to_row(t):
    """Monta o dicionário alinhado com a tabela visualizacao_atual.movidesk_tickets_abertos."""
    tid = t.get("id")
    if isinstance(tid, str) and tid.isdigit():
        tid = int(tid)

    owner = t.get("owner") or {}
    responsavel = owner.get("businessName")
    agent_id = coerce_int(owner.get("id"))

    # === lógica do DEV ===
    # clients[0]["businessName"] e, se existir, clients[0]["organization"]["businessName"]
    clients = t.get("clients") or []
    first = clients[0] if isinstance(clients, list) and clients else {}
    first_client_name = (first or {}).get("businessName")
    org = (first or {}).get("organization") or {}

    empresa_id = org.get("id") if isinstance(org, dict) else None
    empresa_nome = (org.get("businessName") if isinstance(org, dict) and org.get("businessName") else first_client_name)
    empresa_codref = org.get("codeReferenceAdditional") if isinstance(org, dict) else None

    row = {
        "id": tid,
        "protocol": t.get("protocol"),
        "subject": t.get("subject"),
        "type": t.get("type"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "owner_team": t.get("ownerTeam"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "created_date": t.get("createdDate"),
        "last_update": t.get("lastUpdate"),
        "responsavel": responsavel,
        "empresa_cod_ref_adicional": empresa_codref,
        "agent_id": agent_id,
        "empresa_id": str(empresa_id) if empresa_id is not None else None,
        "empresa_nome": empresa_nome,
        "raw_created_by": psycopg2.extras.Json(t.get("createdBy") or {}),
        "raw_clients": psycopg2.extras.Json(clients),
    }
    return row


def upsert_rows(conn, rows):
    if not rows:
        return
    sql = """
    insert into visualizacao_atual.movidesk_tickets_abertos
    (id,protocol,subject,type,status,base_status,owner_team,service_first_level,created_date,last_update,contagem,
     service_second_level,service_third_level,responsavel,empresa_cod_ref_adicional,agent_id,empresa_id,empresa_nome,
     raw_created_by,raw_clients)
    values (%(id)s,%(protocol)s,%(subject)s,%(type)s,%(status)s,%(base_status)s,%(owner_team)s,%(service_first_level)s,
            %(created_date)s,%(last_update)s,1,
            %(service_second_level)s,%(service_third_level)s,%(responsavel)s,%(empresa_cod_ref_adicional)s,
            %(agent_id)s,%(empresa_id)s,%(empresa_nome)s,%(raw_created_by)s,%(raw_clients)s)
    on conflict (id) do update set
      protocol = excluded.protocol,
      subject = excluded.subject,
      type = excluded.type,
      status = excluded.status,
      base_status = excluded.base_status,
      owner_team = excluded.owner_team,
      service_first_level = excluded.service_first_level,
      created_date = excluded.created_date,
      last_update = excluded.last_update,
      service_second_level = excluded.service_second_level,
      service_third_level = excluded.service_third_level,
      responsavel = excluded.responsavel,
      empresa_cod_ref_adicional = excluded.empresa_cod_ref_adicional,
      agent_id = excluded.agent_id,
      empresa_id = excluded.empresa_id,
      empresa_nome = excluded.empresa_nome,
      raw_created_by = excluded.raw_created_by,
      raw_clients = excluded.raw_clients
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()


def delete_rows_not_in(conn, open_ids):
    """Remove da tabela IDs que não estão mais abertos (fechados/resolvidos/cancelados)."""
    if not open_ids:
        with conn.cursor() as cur:
            cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        conn.commit()
        return

    with conn.cursor() as cur:
        cur.execute("select id from visualizacao_atual.movidesk_tickets_abertos")
        existing = [r[0] for r in cur.fetchall()]

    to_delete = [i for i in existing if i not in set(open_ids)]
    if not to_delete:
        return

    chunk = 1000
    with conn.cursor() as cur:
        for i in range(0, len(to_delete), chunk):
            cur.execute(
                "delete from visualizacao_atual.movidesk_tickets_abertos where id = any(%s)",
                (to_delete[i : i + chunk],),
            )
    conn.commit()


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    open_ids = fetch_open_ids()

    rows = []
    for tid in open_ids:
        t = fetch_ticket_by_id(tid)
        if not isinstance(t, dict):
            continue
        rows.append(map_ticket_to_row(t))

    conn = psycopg2.connect(NEON_DSN)
    try:
        upsert_rows(conn, rows)
        delete_rows_not_in(conn, open_ids)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
