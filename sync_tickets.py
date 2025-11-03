import os
import time
import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def fetch_open_tickets():
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    skip = 0
    select_fields = ",".join([
        "id","protocol","subject","type","status","baseStatus","ownerTeam",
        "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
        "createdDate","lastUpdate"
    ])
    expand = "owner($select=id,businessName),clients($select=id,businessName,personType;$expand=organization($select=id,businessName,codeReferenceAdditional))"
    movi_filter = "(baseStatus ne 'Cancelado' and baseStatus ne 'Resolvido' and baseStatus ne 'Fechado')"
    items = []
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$expand": expand,
            "$filter": movi_filter,
            "$top": top,
            "$skip": skip
        }) or []
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return items

def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
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
    empresa_id = None
    empresa_nome = None
    empresa_codref = None
    org = c0.get("organization") or {}
    if isinstance(org, dict) and org:
        empresa_id = org.get("id")
        empresa_nome = org.get("businessName")
        empresa_codref = org.get("codeReferenceAdditional")
    elif c0.get("personType") == 2:
        empresa_id = c0.get("id")
        empresa_nome = c0.get("businessName")
    if not empresa_nome:
        empresa_nome = first_client_name
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
    tickets = fetch_open_tickets()
    rows = [map_row(t) for t in tickets if isinstance(t, dict)]
    open_ids = [r["id"] for r in rows]
    conn = psycopg2.connect(NEON_DSN)
    try:
        n = upsert_rows(conn, rows)
        if open_ids:
            cleanup_deleted(conn, open_ids)
        print(f"UPSERT: {n} registros.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
