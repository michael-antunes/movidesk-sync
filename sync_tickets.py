import os, requests, psycopg2, psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "")

STATUSES = ["Em atendimento","Aguardando","Novo"]

def fetch_open_ids():
    url = "https://api.movidesk.com/public/v1/tickets"
    ids, skip, top = [], 0, 500
    flt = "(" + " or ".join([f"status eq '{s}'" for s in STATUSES]) + ")"
    while True:
        r = requests.get(url, params={"token": API_TOKEN, "$select":"id", "$filter": flt, "$top": top, "$skip": skip}, timeout=90)
        r.raise_for_status()
        data = r.json() or []
        if not data: break
        for t in data:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            if tid is not None:
                ids.append(tid)
        if len(data) < top: break
        skip += len(data)
    return ids

def fetch_ticket(ticket_id):
    r = requests.get("https://api.movidesk.com/public/v1/tickets", params={"token": API_TOKEN, "id": ticket_id}, timeout=90)
    if r.status_code == 200:
        arr = r.json() or []
        if isinstance(arr, list) and arr:
            return arr[0]
    base = f"https://api.movidesk.com/public/v1/tickets/{ticket_id}"
    p1 = {
        "token": API_TOKEN,
        "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
        "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType;$expand=organization($select=id,businessName,codeReferenceAdditional))"
    }
    r = requests.get(base, params=p1, timeout=90)
    if r.status_code == 200:
        return r.json()
    p2 = {
        "token": API_TOKEN,
        "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
        "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType)"
    }
    r = requests.get(base, params=p2, timeout=90)
    r.raise_for_status()
    return r.json()

def prefer_org_from_clients(clients):
    if not isinstance(clients, list): return None
    for c in clients:
        if not isinstance(c, dict): continue
        o = c.get("organization")
        if isinstance(o, dict):
            oid = o.get("id")
            oname = o.get("businessName")
            ocod = o.get("codeReferenceAdditional")
            if oid or oname or ocod:
                return (str(oid) if oid is not None else None, oname, ocod)
    return None

def fetch_person_org(person_id):
    if not person_id: return None
    pid = str(person_id)
    base = "https://api.movidesk.com/public/v1/persons"
    tries = [
        (f"{base}('{pid}')", {"$select":"id,businessName", "$expand":"organization($select=id,businessName,codeReferenceAdditional)"}),
        (f"{base}('{pid}')", {"$select":"id,businessName", "$expand":"organizations($select=id,businessName,codeReferenceAdditional)"}),
    ]
    if pid.isdigit():
        tries.insert(1, (f"{base}({int(pid)})", {"$select":"id,businessName", "$expand":"organization($select=id,businessName,codeReferenceAdditional)"}))
        tries.insert(3, (f"{base}({int(pid)})", {"$select":"id,businessName", "$expand":"organizations($select=id,businessName,codeReferenceAdditional)"}))
    for url, params in tries:
        params = {"token": API_TOKEN, **params}
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and isinstance(data.get("organization"), dict):
                o = data["organization"]
                return (str(o.get("id")) if o.get("id") is not None else None, o.get("businessName"), o.get("codeReferenceAdditional"))
            if isinstance(data, dict) and isinstance(data.get("organizations"), list) and len(data["organizations"]) == 1:
                o = data["organizations"][0] or {}
                return (str(o.get("id")) if o.get("id") is not None else None, o.get("businessName"), o.get("codeReferenceAdditional"))
            if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
                o = data[0]
                return (str(o.get("id")) if o.get("id") is not None else None, o.get("businessName"), o.get("codeReferenceAdditional"))
        if r.status_code in (400,404): continue
        r.raise_for_status()
    return None

def map_row(t):
    tid = t.get("id")
    if isinstance(tid, str) and tid.isdigit(): tid = int(tid)
    owner = t.get("owner") or {}
    responsavel = owner.get("businessName")
    agid = owner.get("id")
    try:
        agent_id = int(agid) if str(agid).isdigit() else None
    except Exception:
        agent_id = None
    empresa_id = None
    empresa_nome = None
    empresa_codref = None
    org = prefer_org_from_clients(t.get("clients"))
    if not org:
        cb = (t.get("createdBy") or {}).get("id")
        org = fetch_person_org(cb)
        if not org:
            clis = t.get("clients") or []
            if clis and isinstance(clis[0], dict):
                org = fetch_person_org(clis[0].get("id"))
    if org:
        empresa_id, empresa_nome, empresa_codref = org
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
        "empresa_id": empresa_id,
        "empresa_nome": empresa_nome,
        "raw_created_by": psycopg2.extras.Json(t.get("createdBy") or {}),
        "raw_clients": psycopg2.extras.Json(t.get("clients") or []),
    }
    return row

def upsert_rows(conn, rows):
    if not rows: return
    sql = """
insert into visualizacao_atual.movidesk_tickets_abertos
(id,protocol,subject,type,status,base_status,owner_team,service_first_level,created_date,last_update,contagem,service_second_level,service_third_level,responsavel,empresa_cod_ref_adicional,agent_id,empresa_id,empresa_nome,raw_created_by,raw_clients)
values (%(id)s,%(protocol)s,%(subject)s,%(type)s,%(status)s,%(base_status)s,%(owner_team)s,%(service_first_level)s,%(created_date)s,%(last_update)s,1,%(service_second_level)s,%(service_third_level)s,%(responsavel)s,%(empresa_cod_ref_adicional)s,%(agent_id)s,%(empresa_id)s,%(empresa_nome)s,%(raw_created_by)s,%(raw_clients)s)
on conflict (id) do update set
protocol=excluded.protocol,
subject=excluded.subject,
type=excluded.type,
status=excluded.status,
base_status=excluded.base_status,
owner_team=excluded.owner_team,
service_first_level=excluded.service_first_level,
created_date=excluded.created_date,
last_update=excluded.last_update,
service_second_level=excluded.service_second_level,
service_third_level=excluded.service_third_level,
responsavel=excluded.responsavel,
empresa_cod_ref_adicional=excluded.empresa_cod_ref_adicional,
agent_id=excluded.agent_id,
empresa_id=excluded.empresa_id,
empresa_nome=excluded.empresa_nome,
raw_created_by=excluded.raw_created_by,
raw_clients=excluded.raw_clients
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

def delete_closed_not_in(conn, open_ids):
    if not open_ids:
        with conn.cursor() as cur:
            cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        conn.commit()
        return
    with conn.cursor() as cur:
        cur.execute("select id from visualizacao_atual.movidesk_tickets_abertos")
        existing = [r[0] for r in cur.fetchall()]
    s = set(open_ids)
    to_delete = [i for i in existing if i not in s]
    if not to_delete: return
    chunk = 500
    with conn.cursor() as cur:
        for k in range(0, len(to_delete), chunk):
            cur.execute("delete from visualizacao_atual.movidesk_tickets_abertos where id = any(%s)", (to_delete[k:k+chunk],))
    conn.commit()

def main():
    if not API_TOKEN or not DSN:
        raise RuntimeError("MOVIDESK_TOKEN e NEON_DSN obrigatórios")
    ids = fetch_open_ids()
    rows = []
    for tid in ids:
        t = fetch_ticket(tid)
        rows.append(map_row(t))
    conn = psycopg2.connect(DSN)
    upsert_rows(conn, rows)
    delete_closed_not_in(conn, ids)
    conn.close()

if __name__ == "__main__":
    main()
