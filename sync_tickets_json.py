import os
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def fetch_tickets_open():
    url = "https://api.movidesk.com/public/v1/tickets"
    out = []
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
        out.extend(batch)
        if len(batch) < top:
            break
        skip += len(batch)
    return out

def get_person_enriched(pid):
    base = "https://api.movidesk.com/public/v1/persons"
    key = f"({int(pid)})" if str(pid).isdigit() else f"('{pid}')"
    url = f"{base}{key}"
    params = {
        "token": API_TOKEN,
        "$select": "id,businessName,profileType,personType,email",
        "$expand": "organization($select=id,businessName,codeReferenceAdditional)"
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code == 404:
        return None
    if r.status_code >= 400:
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    return r.json()

def enrich_people(ids):
    ids = [i for i in ids if i is not None]
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(get_person_enriched, pid): pid for pid in ids}
        for f in as_completed(fut):
            pid = fut[f]
            try:
                out[str(pid)] = f.result()
            except Exception:
                out[str(pid)] = None
    return out

def build_json_records(tickets):
    person_ids = set()
    for t in tickets:
        cb = (t.get("createdBy") or {}).get("id")
        if cb is not None:
            person_ids.add(str(cb))
        for c in t.get("clients") or []:
            cid = c.get("id")
            if cid is not None:
                person_ids.add(str(cid))
    people = enrich_people(sorted(person_ids))
    records = []
    for t in tickets:
        cb = t.get("createdBy") or {}
        cb_enriched = people.get(str(cb.get("id"))) if cb.get("id") is not None else None
        clients_enriched = []
        for c in t.get("clients") or []:
            ce = people.get(str(c.get("id"))) if c.get("id") is not None else None
            clients_enriched.append(ce if ce is not None else c)
        full = {
            "ticket": t,
            "enrichment": {
                "createdBy": cb_enriched if cb_enriched is not None else cb,
                "clients": clients_enriched
            }
        }
        records.append({"id": t["id"], "raw": full})
    return records

def upsert_json(conn, records):
    if not records:
        return
    sql = """
INSERT INTO visualizacao_atual.movidesk_tickets_json (id, raw)
VALUES (%(id)s, %(raw)s)
ON CONFLICT (id) DO UPDATE SET raw = EXCLUDED.raw;
"""
    rows = [{"id": r["id"], "raw": psycopg2.extras.Json(r["raw"])} for r in records]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

def cleanup_resolvidos(conn):
    with conn.cursor() as cur:
        cur.execute("""
DELETE FROM visualizacao_atual.movidesk_tickets_json t
USING visualizacao_resolvidos.tickets_resolvidos r
WHERE r.ticket_id = t.id
""")
    conn.commit()

def main():
    if not API_TOKEN or not DSN:
        raise RuntimeError("Variáveis MOVIDESK_TOKEN e NEON_DSN obrigatórias")
    tickets = fetch_tickets_open()
    records = build_json_records(tickets)
    conn = psycopg2.connect(DSN)
    upsert_json(conn, records)
    cleanup_resolvidos(conn)
    conn.close()

if __name__ == "__main__":
    main()
