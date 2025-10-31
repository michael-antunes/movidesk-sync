import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def _to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def _norm(s):
    if s is None:
        return None
    return str(s).strip().lower()

def _chunk(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def load_empresas_info(conn):
    q = "SELECT id::text AS id_txt, businessname FROM visualizacao_empresa.empresas WHERE businessname IS NOT NULL"
    name_to_id = {}
    id_set = set()
    with conn.cursor() as cur:
        cur.execute(q)
        for id_txt, bn in cur.fetchall():
            id_set.add(str(id_txt))
            n = _norm(bn)
            if n and id_txt:
                name_to_id[n] = str(id_txt)
    return name_to_id, id_set

def _build_filter_ids(ids):
    parts = []
    for pid in ids:
        try:
            int_pid = int(pid)
            parts.append(f"id eq {int_pid}")
        except Exception:
            parts.append(f"id eq '{pid}'")
    return " or ".join(parts)

def fetch_person_org_map(person_ids):
    url = "https://api.movidesk.com/public/v1/persons"
    org_by_person = {}
    ids_list = [pid for pid in person_ids if pid is not None]
    for chunk in _chunk(ids_list, 25):
        params = {
            "token": API_TOKEN,
            "$select": "id",
            "$expand": "organization($select=id,businessName,codeReferenceAdditional)",
            "$filter": _build_filter_ids(chunk)
        }
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            params = {
                "token": API_TOKEN,
                "$select": "id,organizationId,organizationBusinessName,organizationCodeReferenceAdditional",
                "$filter": _build_filter_ids(chunk)
            }
            r = requests.get(url, params=params, timeout=90)
            if r.status_code >= 400:
                params = {
                    "token": API_TOKEN,
                    "$filter": _build_filter_ids(chunk)
                }
                r = requests.get(url, params=params, timeout=90)
                if r.status_code >= 400:
                    raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
        arr = r.json()
        for p in arr:
            pid = str(p.get("id"))
            org = p.get("organization") or {}
            org_id = org.get("id")
            org_name = org.get("businessName")
            if not org_id:
                org_id = p.get("organizationId")
            if not org_name:
                org_name = p.get("organizationBusinessName")
            org_code_ref = org.get("codeReferenceAdditional") or p.get("organizationCodeReferenceAdditional")
            if pid:
                org_by_person[pid] = {
                    "org_id": str(org_id) if org_id is not None else None,
                    "org_name": org_name,
                    "org_code_ref": org_code_ref
                }
    return org_by_person

def fetch_tickets():
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN vazio")
    url = "https://api.movidesk.com/public/v1/tickets"
    out = []
    person_ids = set()
    skip = 0
    top = 500
    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner,clients,createdBy",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break
        for t in batch:
            owner = t.get("owner") or {}
            responsavel = owner.get("businessName")
            owner_id = _to_int(owner.get("id"))
            clients = t.get("clients") or []
            created_by = t.get("createdBy") or {}
            for c in clients:
                cid = c.get("id")
                if cid is not None:
                    person_ids.add(str(cid))
            if created_by.get("id") is not None:
                person_ids.add(str(created_by.get("id")))
            out.append({
                "t": t,
                "responsavel": responsavel,
                "owner_id": owner_id,
                "clients": clients,
                "created_by": created_by
            })
        if len(batch) < top:
            break
        skip += len(batch)
    return out, person_ids

def compute_rows(tickets_raw, person_org, empresas_name_to_id, empresas_id_set):
    rows = []
    for item in tickets_raw:
        t = item["t"]
        empresa_id = None
        for c in item["clients"]:
            cid = c.get("id")
            if cid is None:
                continue
            info = person_org.get(str(cid)) or {}
            oid = info.get("org_id")
            oname = info.get("org_name")
            if oid and oid in empresas_id_set:
                empresa_id = oid
                break
            if not empresa_id and oname:
                mid = empresas_name_to_id.get(_norm(oname))
                if mid:
                    empresa_id = mid
                    break
        if not empresa_id:
            cb = item["created_by"]
            cid = cb.get("id")
            if cid is not None:
                info = person_org.get(str(cid)) or {}
                oid = info.get("org_id")
                oname = info.get("org_name")
                if oid and oid in empresas_id_set:
                    empresa_id = oid
                elif oname:
                    mid = empresas_name_to_id.get(_norm(oname))
                    if mid:
                        empresa_id = mid
        tdict = {
            "id": t["id"],
            "protocol": t.get("protocol"),
            "type": t.get("type"),
            "subject": t.get("subject"),
            "status": t.get("status"),
            "base_status": t.get("baseStatus"),
            "owner_team": t.get("ownerTeam"),
            "service_first_level": t.get("serviceFirstLevel"),
            "service_second_level": t.get("serviceSecondLevel"),
            "service_third_level": t.get("serviceThirdLevel"),
            "created_date": t.get("createdDate"),
            "last_update": t.get("lastUpdate"),
            "responsavel": item["responsavel"],
            "agent_id": item["owner_id"],
            "empresa_id": str(empresa_id) if empresa_id is not None else None
        }
        rows.append(tdict)
    return rows

def upsert_tickets(conn, rows):
    if not rows:
        return
    sql = """
INSERT INTO visualizacao_atual.movidesk_tickets_abertos
  (id, protocol, subject, type, status, base_status, owner_team,
   service_first_level, service_second_level, service_third_level,
   created_date, last_update, responsavel, agent_id, empresa_id)
VALUES
  (%(id)s, %(protocol)s, %(subject)s, %(type)s, %(status)s, %(base_status)s, %(owner_team)s,
   %(service_first_level)s, %(service_second_level)s, %(service_third_level)s,
   %(created_date)s, %(last_update)s, %(responsavel)s, %(agent_id)s, %(empresa_id)s)
ON CONFLICT (id) DO UPDATE SET
  protocol = EXCLUDED.protocol,
  subject = EXCLUDED.subject,
  type = EXCLUDED.type,
  status = EXCLUDED.status,
  base_status = EXCLUDED.base_status,
  owner_team = EXCLUDED.owner_team,
  service_first_level = EXCLUDED.service_first_level,
  service_second_level = EXCLUDED.service_second_level,
  service_third_level = EXCLUDED.service_third_level,
  created_date = EXCLUDED.created_date,
  last_update = EXCLUDED.last_update,
  responsavel = EXCLUDED.responsavel,
  agent_id = EXCLUDED.agent_id,
  empresa_id = EXCLUDED.empresa_id;
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

def cleanup_resolvidos(conn):
    with conn.cursor() as cur:
        cur.execute("""
DELETE FROM visualizacao_atual.movidesk_tickets_abertos t
USING visualizacao_resolvidos.tickets_resolvidos r
WHERE r.ticket_id = t.id
""")
    conn.commit()

def backfill_cod_ref(conn):
    sql = """
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE e.id::text = t.empresa_id
  AND t.empresa_cod_ref_adicional IS DISTINCT FROM e.codereferenceadditional;
"""
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    empresas_name_to_id, empresas_id_set = load_empresas_info(conn)
    tickets_raw, person_ids = fetch_tickets()
    person_org = {}
    if person_ids:
        person_org = fetch_person_org_map(person_ids)
    rows = compute_rows(tickets_raw, person_org, empresas_name_to_id, empresas_id_set)
    upsert_tickets(conn, rows)
    cleanup_resolvidos(conn)
    backfill_cod_ref(conn)
    conn.close()

if __name__ == "__main__":
    main()
