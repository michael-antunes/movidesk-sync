import os, json
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()
STATUSES = ["Em atendimento","Aguardando","Novo"]
PERSON_CACHE = {}

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
        {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": "owner($select=id,businessName),createdBy($select=id,businessName),clients($select=id,businessName,personType,profileType;$expand=organization($select=id,businessName))"
        },
        {
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

def fetch_person_payload(pid):
    if pid in PERSON_CACHE:
        return PERSON_CACHE[pid]
    pid_str = str(pid)
    base = "https://api.movidesk.com/public/v1/persons"
    candidates = []
    candidates.append((f"{base}('{pid_str}')", {"$select":"id,businessName","$expand":"organization($select=id,businessName,codeReferenceAdditional)"}))
    if pid_str.isdigit():
        candidates.append((f"{base}({int(pid_str)})", {"$select":"id,businessName","$expand":"organization($select=id,businessName,codeReferenceAdditional)"}))
    candidates.append((f"{base}('{pid_str}')", {"$select":"id,businessName","$expand":"organizations($select=id,businessName,codeReferenceAdditional)"}))
    if pid_str.isdigit():
        candidates.append((f"{base}({int(pid_str)})", {"$select":"id,businessName","$expand":"organizations($select=id,businessName,codeReferenceAdditional)"}))
    candidates.append((f"{base}('{pid_str}')/organization", {"$select":"id,businessName,codeReferenceAdditional"}))
    if pid_str.isdigit():
        candidates.append((f"{base}({int(pid_str)})/organization", {"$select":"id,businessName,codeReferenceAdditional"}))
    candidates.append((f"{base}('{pid_str}')/organizations", {"$select":"id,businessName,codeReferenceAdditional"}))
    if pid_str.isdigit():
        candidates.append((f"{base}({int(pid_str)})/organizations", {"$select":"id,businessName,codeReferenceAdditional"}))
    for url, extra in candidates:
        params = {"token": API_TOKEN}
        params.update(extra)
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                continue
            PERSON_CACHE[pid] = (data, url)
            return PERSON_CACHE[pid]
        if r.status_code in (400,404):
            continue
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    PERSON_CACHE[pid] = (None, None)
    return PERSON_CACHE[pid]

def select_org_from_person_payload(payload):
    if payload is None:
        return None
    if isinstance(payload, dict):
        if isinstance(payload.get("organization"), dict):
            o = payload["organization"]
            return {"id": o.get("id"), "businessName": o.get("businessName"), "codeReferenceAdditional": o.get("codeReferenceAdditional")}
        orgs = payload.get("organizations")
        if isinstance(orgs, list) and len(orgs) == 1:
            o = orgs[0] or {}
            return {"id": o.get("id"), "businessName": o.get("businessName"), "codeReferenceAdditional": o.get("codeReferenceAdditional")}
        if set(payload.keys()) <= {"id","businessName","codeReferenceAdditional"}:
            return {"id": payload.get("id"), "businessName": payload.get("businessName"), "codeReferenceAdditional": payload.get("codeReferenceAdditional")}
    if isinstance(payload, list):
        items = [x for x in payload if isinstance(x, dict)]
        if len(items) == 1:
            o = items[0]
            return {"id": o.get("id"), "businessName": o.get("businessName"), "codeReferenceAdditional": o.get("codeReferenceAdditional")}
    return None

def enrich_clients_org(ticket_obj):
    clients = ticket_obj.get("clients") or []
    need = []
    for c in clients:
        if isinstance(c, dict) and c.get("id") is not None:
            if not isinstance(c.get("organization"), dict):
                need.append(str(c["id"]))
    if need:
        with ThreadPoolExecutor(max_workers=8) as ex:
            fut = {ex.submit(fetch_person_payload, pid): pid for pid in need}
            for _ in as_completed(fut):
                pass
    out = []
    for c in clients:
        if not isinstance(c, dict):
            out.append(c); continue
        if isinstance(c.get("organization"), dict):
            out.append(c); continue
        pid = str(c.get("id")) if c.get("id") is not None else None
        payload, _ = PERSON_CACHE.get(pid, (None, None))
        chosen = select_org_from_person_payload(payload)
        if chosen:
            d = dict(c)
            d["organization"] = chosen
            out.append(d)
        else:
            out.append(c)
    return out

def build_record(ticket_obj):
    if ticket_obj is None:
        return None
    t = dict(ticket_obj)
    t["clients"] = enrich_clients_org(t)
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
            role["organization"] = {"id": o.get("id"), "businessName": o.get("businessName"), "codeReferenceAdditional": o.get("codeReferenceAdditional")}
        if key in seen:
            for p in participants:
                if str(p["id"]) == key and "client" not in p["roles"]:
                    p["roles"].append("client")
        else:
            participants.append(role)
            seen.add(key)
    full = {"ticket": t, "participants": participants}
    return {"id": t["id"], "raw": full}

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
create table if not exists visualizacao_atual.movidesk_tickets_json (
  id integer primary key,
  raw jsonb not null,
  last_sync timestamptz not null default now()
)""")
        cur.execute("""
create table if not exists visualizacao_atual.person_org_map (
  person_id text primary key,
  org_id text,
  org_name text,
  org_codref text,
  source text,
  raw jsonb,
  updated_at timestamptz default now()
)""")
        cur.execute("""
create table if not exists visualizacao_atual.person_org_override (
  person_id text primary key,
  org_id text,
  org_name text,
  org_codref text,
  note text,
  updated_at timestamptz default now()
)""")
    conn.commit()

def upsert_tickets(conn, records):
    if not records:
        return
    rows = [{"id": r["id"], "raw": psycopg2.extras.Json(r["raw"])} for r in records]
    sql = """
insert into visualizacao_atual.movidesk_tickets_json (id, raw, last_sync)
values (%(id)s, %(raw)s, now())
on conflict (id) do update set raw = excluded.raw, last_sync = now()
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()

def cleanup_resolved(conn):
    with conn.cursor() as cur:
        cur.execute("""
delete from visualizacao_atual.movidesk_tickets_json t
using visualizacao_resolvidos.tickets_resolvidos r
where r.ticket_id = t.id
""")
    conn.commit()

def upsert_person_map(conn, pids):
    if not pids:
        return
    with conn.cursor() as cur:
        cur.execute("select person_id from visualizacao_atual.person_org_override")
        override_ids = {r[0] for r in cur.fetchall()}
    todo = [pid for pid in pids if pid not in override_ids]
    rows = []
    for pid in todo:
        payload, src = fetch_person_payload(pid)
        org = select_org_from_person_payload(payload)
        rows.append({
            "person_id": pid,
            "org_id": org.get("id") if org else None,
            "org_name": org.get("businessName") if org else None,
            "org_codref": org.get("codeReferenceAdditional") if org else None,
            "source": src or None,
            "raw": psycopg2.extras.Json(payload) if payload is not None else None
        })
    if not rows:
        return
    sql = """
insert into visualizacao_atual.person_org_map (person_id, org_id, org_name, org_codref, source, raw, updated_at)
values (%(person_id)s, %(org_id)s, %(org_name)s, %(org_codref)s, %(source)s, %(raw)s, now())
on conflict (person_id) do update
set org_id = excluded.org_id,
    org_name = excluded.org_name,
    org_codref = excluded.org_codref,
    source = excluded.source,
    raw = excluded.raw,
    updated_at = now()
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
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
    all_persons = set()
    for t in details:
        rec = build_record(t)
        if rec:
            records.append(rec)
            raw = rec["raw"]
            for p in raw.get("participants") or []:
                pid = p.get("id")
                if pid:
                    all_persons.add(str(pid))
    conn = psycopg2.connect(DSN)
    ensure_tables(conn)
    upsert_tickets(conn, records)
    upsert_person_map(conn, sorted(all_persons))
    cleanup_resolved(conn)
    conn.close()

if __name__ == "__main__":
    main()
