import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
ONLY_TICKET_ID = os.getenv("ONLY_TICKET_ID")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    r = http.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def list_open_ticket_ids():
    # usa baseStatus para pegar tudo que não está finalizado
    url = "https://api.movidesk.com/public/v1/tickets"
    ids, skip, top = [], 0, 1000
    movi_filter = "(baseStatus ne 'Cancelado' and baseStatus ne 'Resolvido' and baseStatus ne 'Fechado')"
    while True:
        data = _req(url, {
            "token": API_TOKEN,
            "$select": "id",
            "$filter": movi_filter,
            "$top": top,
            "$skip": skip
        }) or []
        if not data:
            break
        for t in data:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            if tid is not None:
                ids.append(tid)
        if len(data) < top:
            break
        skip += len(data)
    return ids

def get_ticket_full(ticket_id):
    # igual ao Dev: /tickets?token&id={id}
    url = "https://api.movidesk.com/public/v1/tickets"
    data = _req(url, {"token": API_TOKEN, "id": ticket_id})
    if isinstance(data, list) and data:
        return data[0]
    return None

def get_person_organizations(person_id):
    url = "https://api.movidesk.com/public/v1/persons"
    data = _req(url, {
        "token": API_TOKEN,
        "$select": "id,businessName",
        "$filter": f"id eq '{person_id}'",
        "$expand": "organizations($select=id,businessName,codeReferenceAdditional)"
    }) or []
    if not data or not isinstance(data, list):
        return []
    orgs = (data[0] or {}).get("organizations") or []
    return orgs if isinstance(orgs, list) else []

def iint(x):
    try:
        return int(x) if str(x).isdigit() else None
    except Exception:
        return None

def map_row(t):
    tid = t.get("id")
    if isinstance(tid, str) and tid.isdigit():
        tid = int(tid)

    owner = t.get("owner") or {}
    responsavel = owner.get("businessName")
    agent_id = iint(owner.get("id"))

    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    first_client_name = c0.get("businessName")
    first_client_id = c0.get("id")

    org = (c0.get("organization") or {}) if isinstance(c0.get("organization"), dict) else {}
    empresa_id = org.get("id")
    empresa_nome = org.get("businessName")
    empresa_codref = org.get("codeReferenceAdditional")

    decision = "ticket.organization"
    if not empresa_id and first_client_id:
        orgs = get_person_organizations(first_client_id)
        if orgs:
            o = orgs[0] or {}
            empresa_id = o.get("id")
            empresa_nome = o.get("businessName")
            empresa_codref = o.get("codeReferenceAdditional")
            decision = "persons.organizations[0]"
        else:
            decision = "no-org"

    if not empresa_nome:
        # fallback igual ao DEV
        empresa_nome = first_client_name

    print(f"[ticket {tid}] dec={decision} emp_id={empresa_id} emp_nome={empresa_nome}")

    return {
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

UPSERT_SQL = """
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

def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)

def cleanup_deleted(conn, open_ids):
    if not open_ids:
        with conn.cursor() as cur:
            cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute("delete from visualizacao_atual.movidesk_tickets_abertos where id <> all(%s)", (open_ids,))
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    if ONLY_TICKET_ID:
        ids = [int(ONLY_TICKET_ID)]
        print(f"ONLY_TICKET_ID={ONLY_TICKET_ID} (modo teste)")
    else:
        ids = list_open_ticket_ids()
        print(f"Total de tickets abertos: {len(ids)}")

    rows = []
    for tid in ids:
        t = get_ticket_full(tid)
        if isinstance(t, dict):
            rows.append(map_row(t))

    conn = psycopg2.connect(NEON_DSN)
    try:
        n = upsert_rows(conn, rows)
        print(f"UPSERT concluído: {n} registros.")
        if not ONLY_TICKET_ID:
            cleanup_deleted(conn, ids)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
