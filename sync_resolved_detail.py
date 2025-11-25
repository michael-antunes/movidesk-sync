import os
import time
import json
import requests
import psycopg2
import psycopg2.extras

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.3"))
BATCH_LIMIT = int(os.getenv("MOVIDESK_DETAIL_BATCH_LIMIT", "100"))
DETAIL_TABLE = os.getenv("MOVIDESK_DETAIL_TABLE", "visualizacao_resolvidos.tickets_resolvidos_detail")

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def _req(url, params=None, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:1200])
            except Exception:
                pass
            r.raise_for_status()
        return r.json() if r.text else None


def fetch_ticket_detail(ticket_id):
    url = f"{API_BASE}/tickets"
    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        "includeDeletedItems": "true",
    }
    return _req(url, params=params, timeout=120) or {}


def map_row(t):
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    first_client = clients[0] if clients else {}
    org = first_client.get("organization") or {}
    return {
        "ticket_id": t.get("id"),
        "protocol": t.get("protocol"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "justification": t.get("justification"),
        "subject": t.get("subject"),
        "type": t.get("type"),
        "origin": t.get("origin"),
        "created_date": t.get("createdDate"),
        "resolved_date": t.get("resolvedIn") or t.get("resolvedDate"),
        "closed_date": t.get("closedIn") or t.get("closedDate"),
        "client_id": first_client.get("id"),
        "client_name": first_client.get("businessName"),
        "organization_id": org.get("id"),
        "organization_name": org.get("businessName"),
        "owner_id": owner.get("id"),
        "owner_name": owner.get("businessName"),
        "owner_team_name": t.get("ownerTeam"),
        "raw_json": t,
    }


UPSERT_SQL = f"""
INSERT INTO {DETAIL_TABLE} (
    ticket_id,
    protocol,
    status,
    base_status,
    justification,
    subject,
    type,
    origin,
    created_date,
    resolved_date,
    closed_date,
    client_id,
    client_name,
    organization_id,
    organization_name,
    owner_id,
    owner_name,
    owner_team_name,
    raw_json
) VALUES (
    %(ticket_id)s,
    %(protocol)s,
    %(status)s,
    %(base_status)s,
    %(justification)s,
    %(subject)s,
    %(type)s,
    %(origin)s,
    %(created_date)s,
    %(resolved_date)s,
    %(closed_date)s,
    %(client_id)s,
    %(client_name)s,
    %(organization_id)s,
    %(organization_name)s,
    %(owner_id)s,
    %(owner_name)s,
    %(owner_team_name)s,
    %(raw_json)s::jsonb
)
ON CONFLICT (ticket_id) DO UPDATE SET
    protocol = EXCLUDED.protocol,
    status = EXCLUDED.status,
    base_status = EXCLUDED.base_status,
    justification = EXCLUDED.justification,
    subject = EXCLUDED.subject,
    type = EXCLUDED.type,
    origin = EXCLUDED.origin,
    created_date = EXCLUDED.created_date,
    resolved_date = EXCLUDED.resolved_date,
    closed_date = EXCLUDED.closed_date,
    client_id = EXCLUDED.client_id,
    client_name = EXCLUDED.client_name,
    organization_id = EXCLUDED.organization_id,
    organization_name = EXCLUDED.organization_name,
    owner_id = EXCLUDED.owner_id,
    owner_name = EXCLUDED.owner_name,
    owner_team_name = EXCLUDED.owner_team_name,
    raw_json = EXCLUDED.raw_json;
"""


def _normalize_row_for_db(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def upsert_rows(conn, rows):
    if not rows:
        return 0
    safe_rows = [_normalize_row_for_db(r) for r in rows if isinstance(r, dict)]
    if not safe_rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, safe_rows, page_size=200)
    conn.commit()
    return len(safe_rows)


def select_missing_ticket_ids(conn, limit):
    sql = """
        SELECT ticket_id
        FROM visualizacao_resolvidos.audit_recent_missing
        ORDER BY ticket_id
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def mark_processed(conn, ticket_ids):
    if not ticket_ids:
        return
    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
        WHERE ticket_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_ids,))
    conn.commit()


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    with psycopg2.connect(NEON_DSN) as conn:
        ticket_ids = select_missing_ticket_ids(conn, BATCH_LIMIT)
        if not ticket_ids:
            print("Nenhum ticket pendente para reprocessar.")
            return
        print(f"Reprocessando {len(ticket_ids)} tickets: {ticket_ids}")
        rows = []
        for tid in ticket_ids:
            t = fetch_ticket_detail(tid)
            if not isinstance(t, dict) or t.get("id") is None:
                continue
            rows.append(map_row(t))
            time.sleep(THROTTLE)
        n = upsert_rows(conn, rows)
        mark_processed(conn, ticket_ids)
        print(f"UPSERT detail: {n} linhas atualizadas.")


if __name__ == "__main__":
    main()
