import os, time, json
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

STATUSES = ["Em atendimento","Aguardando","Novo"]

def fetch_open_ticket_ids():
    url = "https://api.movidesk.com/public/v1/tickets"
    out, skip, top = [], 0, 500
    flt = "(" + " or ".join([f"status eq '{s}'" for s in STATUSES]) + ")"
    while True:
        params = {"token": API_TOKEN, "$select": "id", "$filter": flt, "$top": top, "$skip": skip}
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            raise RuntimeError(f"Tickets IDs HTTP {r.status_code}: {r.text}")
        batch = r.json() or []
        if not batch:
            break
        out.extend([int(t["id"]) if isinstance(t.get("id"), str) and t["id"].isdigit() else t["id"] for t in batch])
        if len(batch) < top:
            break
        skip += len(batch)
    return [i for i in out if i is not None]

def fetch_ticket_detail(ticket_id):
    base = f"https://api.movidesk.com/public/v1/tickets/{ticket_id}"
    params_try = [
        {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType;$expand=organization($select=id,businessName,codeReferenceAdditional))"
        },
        {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType)"
        }
    ]
    for p in params_try:
        r = requests.get(base, params=p, timeout=90)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400, 404):
            continue
        raise RuntimeError(f"Ticket {ticket_id} HTTP {r.status_code}: {r.text}")
    return None

def fetch_person_with_org(pid):
    pid_str = str(pid)
    urls = [f"https://api.movidesk.com/public/v1/persons('{pid_str}')"]
    if pid_str.isdigit():
        urls.append(f"https://api.movidesk.com/public/v1/persons({int(pid_str)})")
    params = {"token": API_TOKEN, "$select": "id,businessName,profileType,personType,email", "$expand": "organization($select=id,businessName,codeReferenceAdditional)"}
    for u in urls:
        r = requests.get(u, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400,404):
            continue
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    return None

def enrich_clients_with_org(ticket_obj):
    clients = ticket_obj.get("clients") or []
    need = []
    for c in clients:
        if not isinstance(c, dict):
            continue
        if c.get("organization") is None:
            if c.get("id") is not None:
                need.append(str(c["id"]))
    if not need:
        return clients
    enriched = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(fetch_person_with_org, pid): pid for pid in need}
        for f in as_completed(fut):
            pid = fut[f]
            try:
                enriched[pid] = f.result()
            except Exception:
                enriched[pid] = None
    out = []
    for c in clients:
        if not isinstance(c, dict):
            out.append(c)
            continue
        cid = str(c.get("id")) if c.get("id") is not None else None
        org = c.get("organization")
        if org is None and cid and enriched.get(cid) and isinstance(enriched[cid], dict):
            p = enriched[cid]
            o = p.get("organization") or {}
            if o:
                c = dict(c)
                c["organization"] = {
                    "id": o.get("id"),
                    "businessName": o.get("businessName"),
                    "codeReferenceAdditional": o.get("codeReferenceAdditional")
                }
        out.append(c)
    return out

def build_record(ticket_obj):
    if ticket_obj is None:
        return None
    t = dict(ticket_obj)
    t["clients"] = enrich_clients_with_org(t)
    participants = []
    seen = set()
    cb = t.get("createdBy") or {}
    if cb.get("id") is not None:
        participants.append({"id": cb.get("id"), "businessName": cb.get("businessName"), "roles": ["createdBy"]})
        seen.add(str(cb.get("id")))
    for c in t.get("clients") or []:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        if cid is None:
            continue
        key = str(cid)
        role = {"id": cid, "businessName": c.get("businessName"), "roles": ["client"]}
        if c.get("organization"):
            role["organization"] = {
                "id": c["organization"].get("id"),
                "businessName": c["organization"].get("businessName"),
                "codeReferenceAdditional": c["organization"].get("codeReferenceAdditional")
            }
        if key in seen:
            for p in participants:
                if str(p["id"]) == key and "client" not in p["roles"]:
                    p["roles"].append("client")
        else:
            participants.append(role)
            seen.add(key)
    full = {"ticket": t, "participants": participants}
    return {"id": t["id"], "raw": full}

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
CREATE TABLE IF NOT EXISTS visualizacao_atual.movidesk_tickets_json (
  id integer PRIMARY KEY,
  raw jsonb NOT NULL,
  last_sync timestamptz NOT NULL DEFAULT now()
)
""")
    conn.commit()

def upsert_records(conn, records):
    if not records:
        return
    rows = [{"id": r["id"], "raw": psycopg2.extras.Json(r["raw"])} for r in records]
    sql = """
INSERT INTO visualizacao_atual.movidesk_tickets_json (id, raw, last_sync)
VALUES (%(id)s, %(raw)s, now())
ON CONFLICT (id) DO UPDATE SET raw = EXCLUDED.raw, last_sync = now()
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()

def cleanup_resolved(conn):
    with conn.cursor() as cur:
        cur.execute("""
DELETE FROM visualizacao_atual.movidesk_tickets_json t
USING visualizacao_resolvidos.tickets_resolvidos r
WHERE r.ticket_id = t.id
""")
    conn.commit()

def main():
    if not API_TOKEN or not DSN:
        raise RuntimeError("MOVIDESK_TOKEN e NEON_DSN são obrigatórios")
    ids = fetch_open_ticket_ids()
    details = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(fetch_ticket_detail, i): i for i in ids}
        for f in as_completed(fut):
            try:
                details.append(f.result())
            except Exception:
                pass
    records = []
    for t in details:
        rec = build_record(t)
        if rec:
            records.append(rec)
    conn = psycopg2.connect(DSN)
    ensure_table(conn)
    upsert_records(conn, records)
    cleanup_resolved(conn)
    conn.close()

if __name__ == "__main__":
    main()
