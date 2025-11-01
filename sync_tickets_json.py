import os, json
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
        for t in batch:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            if tid is not None:
                out.append(tid)
        if len(batch) < top:
            break
        skip += len(batch)
    return out

def fetch_ticket_detail(ticket_id):
    base = f"https://api.movidesk.com/public/v1/tickets/{ticket_id}"
    attempts = [
        { # tenta organization no client (sem codeReferenceAdditional)
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType;$expand=organization($select=id,businessName))"
        },
        { # fallback sem organization
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType)"
        }
    ]
    for p in attempts:
        r = requests.get(base, params=p, timeout=90)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400,404):
            continue
        raise RuntimeError(f"Ticket {ticket_id} HTTP {r.status_code}: {r.text}")
    return None

def fetch_person_with_orgs(pid):
    pid_str = str(pid)
    urls = [
        f"https://api.movidesk.com/public/v1/persons('{pid_str}')"
    ]
    if pid_str.isdigit():
        urls.append(f"https://api.movidesk.com/public/v1/persons({int(pid_str)})")
    params = {
        "token": API_TOKEN,
        "$select": "id,businessName,profileType,personType,email",
        "$expand": "organizations($select=id,businessName,codeReferenceAdditional)"
    }
    for u in urls:
        r = requests.get(u, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400,404):
            continue
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    return None

def choose_org_from_person(p):
    if not isinstance(p, dict):
        return None
    orgs = p.get("organizations")
    if isinstance(orgs, list) and len(orgs) == 1:
        o = orgs[0] or {}
        return {
            "id": o.get("id"),
            "businessName": o.get("businessName"),
            "codeReferenceAdditional": o.get("codeReferenceAdditional")
        }
    return None

def enrich_clients_org_if_needed(ticket_obj):
    clients = ticket_obj.get("clients") or []
    need = []
    for c in clients:
        if isinstance(c, dict) and c.get("id") is not None:
            if not isinstance(c.get("organization"), dict):
                need.append(str(c["id"]))
    if not need:
        return clients
    enriched = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(fetch_person_with_orgs, pid): pid for pid in need}
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
        if isinstance(c.get("organization"), dict):
            out.append(c)
            continue
        cid = c.get("id")
        p = enriched.get(str(cid))
        chosen = choose_org_from_person(p)
        if chosen:
            c = dict(c)
            c["organization"] = chosen
        out.append(c)
    return out

def build_record(ticket_obj):
    if ticket_obj is None:
        return None
    t = dict(ticket_obj)
    t["clients"] = enrich_clients_org_if_needed(t)

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
        if isinstance(c.get("organization"), dict):
            o = c["organization"]
            role["organization"] = {
                "id": o.get("id"),
                "businessName": o.get("businessName"),
                "codeReferenceAdditional": o.get("codeReferenceAdditional")
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
