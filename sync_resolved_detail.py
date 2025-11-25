import os
import time
import requests
import psycopg2
import psycopg2.extras

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.3"))
BATCH_LIMIT = int(os.getenv("MOVIDESK_DETAIL_BATCH_LIMIT", "100"))


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def _req(url, params=None, timeout=90):
    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
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
    data = _req(url, params=params, timeout=120)
    if isinstance(data, list):
        return data[0] if data else {}
    if isinstance(data, dict) and data.get("id") is not None:
        return data
    return {}


def map_row(t):
    owner = t.get("owner") or {}
    return {
        "ticket_id": iint(t.get("id")),
        "status": t.get("status"),
        "owner_name": owner.get("businessName") or owner.get("name"),
        "owner_team_name": t.get("ownerTeam"),
        "origin": t.get("origin"),
    }


UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,owner_name,owner_team_name,origin)
values (%(ticket_id)s,%(status)s,%(owner_name)s,%(owner_team_name)s,%(origin)s)
on conflict (ticket_id) do update set
  status = excluded.status,
  owner_name = excluded.owner_name,
  owner_team_name = excluded.owner_team_name,
  origin = excluded.origin
"""


DELETE_MISSING_SQL = """
delete from visualizacao_resolvidos.audit_recent_missing
where table_name = 'tickets_resolvidos'
  and ticket_id = %s
"""


def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def select_missing_ticket_ids(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct ticket_id
            from visualizacao_resolvidos.audit_recent_missing
            where table_name = 'tickets_resolvidos'
            order by ticket_id desc
            limit %s
        """,
            (limit,),
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


def mark_processed(conn, ticket_ids):
    if not ticket_ids:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            DELETE_MISSING_SQL,
            [(tid,) for tid in ticket_ids],
            page_size=200,
        )
    conn.commit()
    return len(ticket_ids)


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    conn = psycopg2.connect(NEON_DSN)
    try:
        ticket_ids = select_missing_ticket_ids(conn, BATCH_LIMIT)
        if not ticket_ids:
            print("Nenhum ticket pendente para reprocessar.")
            return
        print(f"Reprocessando {len(ticket_ids)} tickets: {ticket_ids}")
        rows = []
        reprocessed_ids = []
        for tid in ticket_ids:
            t = fetch_ticket_detail(tid)
            if not isinstance(t, dict) or t.get("id") is None:
                continue
            row = map_row(t)
            if row.get("ticket_id") is None:
                continue
            if row.get("status") is None:
                continue
            rows.append(row)
            reprocessed_ids.append(tid)
            time.sleep(THROTTLE)
        n_upsert = upsert_rows(conn, rows)
        n_delete = mark_processed(conn, reprocessed_ids)
        print(f"UPSERT detail: {n_upsert} linhas atualizadas.")
        print(f"DELETE MISSING: {n_delete}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
