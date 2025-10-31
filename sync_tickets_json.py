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
            " $expand ": None  # placeholder, será removido já já
        }
        # usamos uma string de expand robusta:
        # - owner: apenas id/nome
        # - clients: SEM $select (para não perder 'organization')
        # - createdBy: apenas id/nome
        params["$expand"] = "owner($select=id,businessName),clients,createdBy($select=id,businessName)"
        params["$filter"] = "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')"
        params["$top"] = top
        params["$skip"] = skip

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
    # coleto IDs a enriquecer (só quando o ticket não trouxe organization)
    person_ids = set()
    for t in tickets:
        cb = (t.get("createdBy") or {}).get("id")
        if cb is not None:
            person_ids.add(str(cb))
        for c in t.get("clients") or []:
            if isinstance(c, dict):
                cid = c.get("id")
                has_org = isinstance(c.get("organization"), dict)
                if cid is not None and not has_org:
                    person_ids.add(str(cid))

    people = enrich_people(sorted(person_ids))
    records = []

    for t in tickets:
        # mapa para deduplicar participantes e agregar "roles"
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
            # base do ticket (pode já ter organization dentro de clients)
            from_ticket = None
            for c in t.get("clients") or []:
                if isinstance(c, dict) and str(c.get("id")) == pid:
                    from_ticket = c
                    break
            # enriquecimento (se precisamos)
            p_enriched = people.get(pid)
            org_obj = None

            if from_ticket and isinstance(from_ticket.get("organization"), dict):
                org = from_ticket["organization"]
                org_obj = {
                    "id": org.get("id"),
                    "businessName": org.get("businessName"),
                    "codeReferenceAdditional": org.get("codeReferenceAdditional")
                }

            if org_obj is None and isinstance(p_enriched, dict):
                org = p_enriched.get("organization") or {}
                if org:
                    org_obj = {
                        "id": org.get("id"),
                        "businessName": org.get("businessName"),
                        "codeReferenceAdditional": org.get("codeReferenceAdditional")
                    }

            # dados pessoa
            p_name = None
            p_email = None
            p_pt = None
            p_pr = None
            if isinstance(p_enriched, dict):
                p_name = p_enriched.get("businessName")
                p_email = p_enriched.get("email")
                p_pt = p_enriched.get("personType")
                p_pr = p_enriched.get("profileType")

            if not p_name and from_ticket:
                p_name = from_ticket.get("businessName")

            participants.append({
                "id": p_enriched.get("id") if isinstance(p_enriched, dict) and p_enriched.get("id") is not None else pid,
                "businessName": p_name or info.get("fallback_name"),
                "profileType": p_pr,
                "personType": p_pt,
                "email": p_email,
                "organization": org_obj,
                "roles": sorted(list(info["roles"]))
            })

        full = {
            "ticket": t,              # ticket como veio do endpoint (com clients->organization quando disponível)
            "participants": participants  # participantes deduplicados com organização
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
