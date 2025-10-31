import os
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def fetch_tickets_open():
    url = "https://api.movidesk.com/public/v1/tickets"
    out, skip, top = [], 0, 500
    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            "$expand": (
                "owner($select=id,businessName),"
                "createdBy($select=id,businessName),"
                "clients($select=id,businessName,personType,profileType),"
                "clients/organization($select=id,businessName)"
            ),
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
    pid_str = str(pid)
    urls = [f"{base}('{pid_str}')"]
    if pid_str.isdigit():
        urls.append(f"{base}({int(pid_str)})")
    params = {
        "token": API_TOKEN,
        "$select": "id,businessName,profileType,personType,email",
        "$expand": "organization($select=id,businessName,codeReferenceAdditional)"
    }
    for url in urls:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400, 404):
            continue
        raise RuntimeError(f"Persons HTTP {r.status_code}: {r.text}")
    return None

def enrich_people(ids):
    ids = [i for i in ids if i is not None]
    out = {}
    if not ids:
        return out
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
            if isinstance(c, dict) and c.get("id") is not None:
                person_ids.add(str(c.get("id")))

    people = enrich_people(sorted(person_ids))
    records = []

    for t in tickets:
        participants_map = {}

        cb = t.get("createdBy") or {}
        cb_id = cb.get("id")
        if cb_id is not None:
            participants_map.setdefault(str(cb_id), {"roles": set(), "fallback_name": cb.get("businessName")})
            participants_map[str(cb_id)]["roles"].add("createdBy")

        for c in t.get("clients") or []:
            if not isinstance(c, dict):
                continue
            cid = c.get("id")
            if cid is None:
                continue
            key = str(cid)
            participants_map.setdefault(key, {"roles": set(), "fallback_name": c.get("businessName")})
            participants_map[key]["roles"].add("client")
            if not participants_map[key]["fallback_name"] and c.get("businessName"):
                participants_map[key]["fallback_name"] = c.get("businessName")

        participants = []
        for pid, info in participants_map.items():
            from_ticket = None
            for c in t.get("clients") or []:
                if isinstance(c, dict) and str(c.get("id")) == pid:
                    from_ticket = c
                    break

            p_enriched = people.get(pid)
            org_ticket = (from_ticket.get("organization") if from_ticket else None) or {}
            org_person = (p_enriched.get("organization") if isinstance(p_enriched, dict) else None) or {}

            org_obj = None
            if org_ticket or org_person:
                org_obj = {
                    "id": org_ticket.get("id") or org_person.get("id"),
                    "businessName": org_ticket.get("businessName") or org_person.get("businessName"),
                    "codeReferenceAdditional": org_person.get("codeReferenceAdditional")
                }

            p_name = (p_enriched.get("businessName") if isinstance(p_enriched, dict) else None) or info.get("fallback_name")
            participants.append({
                "id": (p_enriched.get("id") if isinstance(p_enriched, dict) and p_enriched.get("id") is not None else pid),
                "businessName": p_name,
                "profileType": (p_enriched.get("profileType") if isinstance(p_enriched, dict) else None),
                "personType": (p_enriched.get("personType") if isinstance(p_enriched, dict) else None),
                "email": (p_enriched.get("email") if isinstance(p_enriched, dict) else None),
                "organization": org_obj,
                "roles": sorted(list(info["roles"]))
            })

        full = {"ticket": t, "participants": participants}
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
