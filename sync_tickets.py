import os
import requests
import psycopg2
import psycopg2.extras
from time import sleep
from urllib.parse import quote

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def _to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def fetch_tickets_open():
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
            "$expand": "owner($select=id,businessName),clients($select=id,businessName),createdBy($select=id,businessName)",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            raise RuntimeError(f"Tickets HTTP {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break
        for t in batch:
            owner = t.get("owner") or {}
            responsavel = owner.get("businessName")
            agent_id = _to_int(owner.get("id"))
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
                "agent_id": agent_id,
                "clients": clients,
                "created_by": created_by
            })
        if len(batch) < top:
            break
        skip += len(batch)
    return out, sorted(person_ids)

def fetch_person_org_by_key(person_id):
    base = "https://api.movidesk.com/public/v1/persons"
    is_num = str(person_id).isdigit()
    key = f"({int(person_id)})" if is_num else f"('{quote(str(person_id))}')"
    url = f"{base}{key}"
    params = {
        "token": API_TOKEN,
        "$select": "id",
        "$expand": "organization($select=id,businessName,codeReferenceAdditional)"
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code == 404:
        return None, None, None
    if r.status_code >= 400:
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    obj = r.json() or {}
    org = obj.get("organization") or {}
    return (str(org.get("id")) if org.get("id") is not None else None,
            org.get("businessName"),
            str(org.get("codeReferenceAdditional")) if org.get("codeReferenceAdditional") is not None else None)

def build_person_org_map(person_ids):
    cache = {}
    for pid in person_ids:
        try:
            org_id, org_name, org_coderef = fetch_person_org_by_key(pid)
        except Exception:
            org_id, org_name, org_coderef = None, None, None
        cache[str(pid)] = {"org_id": org_id, "org_name": org_name, "org_coderef": org_coderef}
        sleep(0.05)
    return cache

def compute_rows(tickets_raw, org_by_person):
    rows = []
    for item in tickets_raw:
        t = item["t"]
        empresa_id = None
        empresa_nome = None
        empresa_coderef = None
        cb = item["created_by"] or {}
        cbid = cb.get("id")
        if cbid is not None:
            info = org_by_person.get(str(cbid)) or {}
            empresa_id = info.get("org_id") or empresa_id
            empresa_nome = info.get("org_name") or empresa_nome
            empresa_coderef = info.get("org_coderef") or empresa_coderef
        if not empresa_coderef or not empresa_id or not empresa_nome:
            for c in item["clients"]:
                cid = c.get("id")
                if cid is None:
                    continue
                info = org_by_person.get(str(cid)) or {}
                empresa_id = empresa_id or info.get("org_id")
                empresa_nome = empresa_nome or info.get("org_name")
                empresa_coderef = empresa_coderef or info.get("org_coderef")
                if empresa_coderef and empresa_id and empresa_nome:
                    break
        rows.append({
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
            "agent_id": item["agent_id"],
            "empresa_id": empresa_id,
            "empresa_nome": empresa_nome,
            "empresa_cod_ref_adicional": empresa_coderef
        })
    return rows

def upsert_tickets(conn, rows):
    if not rows:
        return
    sql = """
INSERT INTO visualizacao_atual.movidesk_tickets_abertos
  (id, protocol, subject, type, status, base_status, owner_team,
   service_first_level, service_second_level, service_third_level,
   created_date, last_update, responsavel, agent_id,
   empresa_id, empresa_nome, empresa_cod_ref_adicional)
VALUES
  (%(id)s, %(protocol)s, %(subject)s, %(type)s, %(status)s, %(base_status)s, %(owner_team)s,
   %(service_first_level)s, %(service_second_level)s, %(service_third_level)s,
   %(created_date)s, %(last_update)s, %(responsavel)s, %(agent_id)s,
   %(empresa_id)s, %(empresa_nome)s, %(empresa_cod_ref_adicional)s)
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
  empresa_id = EXCLUDED.empresa_id,
  empresa_nome = EXCLUDED.empresa_nome,
  empresa_cod_ref_adicional = EXCLUDED.empresa_cod_ref_adicional;
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

def backfill_coderef(conn):
    with conn.cursor() as cur:
        cur.execute("""
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE t.empresa_cod_ref_adicional IS NULL
  AND e.id::text = t.empresa_id;
        """)
        cur.execute("""
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE t.empresa_cod_ref_adicional IS NULL
  AND t.empresa_nome IS NOT NULL
  AND e.businessname IS NOT NULL
  AND lower(trim(e.businessname)) = lower(trim(t.empresa_nome));
        """)
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    tickets_raw, person_ids = fetch_tickets_open()
    org_by_person = build_person_org_map(person_ids)
    rows = compute_rows(tickets_raw, org_by_person)
    upsert_tickets(conn, rows)
    cleanup_resolvidos(conn)
    backfill_coderef(conn)
    conn.close()

if __name__ == "__main__":
    main()
