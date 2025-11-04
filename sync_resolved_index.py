import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            wait = int(r.headers.get("retry-after") or 60); time.sleep(wait); continue
        if r.status_code == 404: return []
        r.raise_for_status(); return r.json() if r.text else []

def fetch_index(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","500")); throttle = float(os.getenv("MOVIDESK_THROTTLE","0.2")); skip=0
    select_fields = ",".join([
        "id","baseStatus","status","lastUpdate","createdDate","ownerTeam",
        "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    filtro = "(" + " or ".join([ "baseStatus eq 'Resolved'","baseStatus eq 'Closed'","baseStatus eq 'Canceled'" ]) + f") and lastUpdate ge {since_iso}"
    expand = "owner,clients($expand=organization)"
    out=[]
    while True:
        page = _req(url, {"token":API_TOKEN,"$select":select_fields,"$expand":expand,"$filter":filtro,"$top":top,"$skip":skip}) or []
        if not page: break
        out.extend(page)
        if len(page) < top: break
        skip += len(page); time.sleep(throttle)
    return out

def upsert_index(conn, items):
    sql = """
    insert into visualizacao_resolvidos.tickets_resolvidos
    (id,protocol,subject,type,status,base_status,owner_team,service_first_level,service_second_level,service_third_level,
     created_date,last_update,responsavel,empresa_cod_ref_adicional,agent_id,empresa_id,empresa_nome,raw_owner,raw_clients)
    values
    (%(id)s,%(protocol)s,%(subject)s,%(type)s,%(status)s,%(base_status)s,%(owner_team)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
     %(created_date)s,%(last_update)s,%(responsavel)s,%(empresa_cod_ref_adicional)s,%(agent_id)s,%(empresa_id)s,%(empresa_nome)s,%(raw_owner)s,%(raw_clients)s)
    on conflict (id) do update set
      status = excluded.status,
      base_status = excluded.base_status,
      owner_team = excluded.owner_team,
      service_first_level = excluded.service_first_level,
      service_second_level = excluded.service_second_level,
      service_third_level = excluded.service_third_level,
      created_date = excluded.created_date,
      last_update = excluded.last_update,
      responsavel = excluded.responsavel,
      empresa_cod_ref_adicional = excluded.empresa_cod_ref_adicional,
      agent_id = excluded.agent_id,
      empresa_id = excluded.empresa_id,
      empresa_nome = excluded.empresa_nome,
      raw_owner = excluded.raw_owner,
      raw_clients = excluded.raw_clients;
    """
    ctrl_sql = """
    insert into visualizacao_resolvidos.detail_control (ticket_id,last_update)
    values (%(id)s,%(last_update)s)
    on conflict (ticket_id) do update set last_update = excluded.last_update;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, items, page_size=300)
        psycopg2.extras.execute_batch(cur, ctrl_sql, [{"id":i["id"],"last_update":i["last_update"]} for i in items], page_size=300)
    conn.commit()

def map_row(t):
    def iint(x):
        try: s=str(x); return int(s) if s.isdigit() else None
        except: return None
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if clients else {}
    org = c0.get("organization") or {}
    return {
        "id": int(t.get("id")) if str(t.get("id")).isdigit() else None,
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
        "responsavel": owner.get("businessName"),
        "empresa_cod_ref_adicional": org.get("codeReferenceAdditional"),
        "agent_id": iint(owner.get("id")),
        "empresa_id": str(org.get("id")) if org.get("id") is not None else None,
        "empresa_nome": org.get("businessName") or c0.get("businessName"),
        "raw_owner": psycopg2.extras.Json(owner),
        "raw_clients": psycopg2.extras.Json(clients),
    }

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    days = int(os.getenv("MOVIDESK_RESOLVED_INDEX_DAYS","180"))
    since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    conn = psycopg2.connect(NEON_DSN)
    try:
        items = [map_row(x) for x in fetch_index(since)]
        if items: upsert_index(conn, items)
        with conn.cursor() as cur:
            cur.execute("update visualizacao_resolvidos.sync_control set last_index_cursor = now(), updated_at = now() where id=1")
        conn.commit()
        print(f"INDEX UPSERT: {len(items)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
