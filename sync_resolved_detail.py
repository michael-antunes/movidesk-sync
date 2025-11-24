import os
import time
import datetime
import requests
import psycopg2
import psycopg2.extras

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
REPROCESS_LIMIT = int(os.getenv("MOVIDESK_REPROCESS_LIMIT", "100"))
MAX_HTTP_ERROR_RETRIES = int(os.getenv("MOVIDESK_MAX_HTTP_ERROR_RETRIES", "30"))


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def _req(url, params, timeout=90):
    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if retry and str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:1200])
            except Exception:
                pass
            r.raise_for_status()
        return r.json() if r.text else []


def fetch_ticket(ticket_id):
    url = f"{API_BASE}/tickets"
    params = {
        "token": API_TOKEN,
        "$select": "id,subject,type,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,origin,category,urgency,createdDate,lastUpdate",
        "$expand": "clients($expand=organization),createdBy,owner",
        "$filter": f"id eq {ticket_id}",
        "$top": 1,
        "$skip": 0,
    }
    page = _req(url, params) or []
    return page[0] if page else None


def map_row(t):
    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    org = c0.get("organization") or {}
    created_by = t.get("createdBy") or {}
    owner = t.get("owner") or {}
    return {
        "ticket_id": iint(t.get("id")),
        "owner_name": owner.get("businessName") or owner.get("name"),
        "owner_team_name": t.get("ownerTeam"),
        "origin": t.get("origin"),
        "raw_payload": t,
        "imported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,owner_name,owner_team_name,origin,raw_payload,imported_at)
values (%(ticket_id)s,%(owner_name)s,%(owner_team_name)s,%(origin)s,%(raw_payload)s,%(imported_at)s)
on conflict (ticket_id) do update set
  owner_name = excluded.owner_name,
  owner_team_name = excluded.owner_team_name,
  origin = excluded.origin,
  raw_payload = excluded.raw_payload,
  imported_at = excluded.imported_at
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


def delete_missing(conn, ticket_ids):
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


def get_missing_tickets(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct ticket_id
            from visualizacao_resolvidos.audit_recent_missing
            where table_name = 'tickets_resolvidos'
            order by ticket_id
            limit %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


def register_http_error(conn, ticket_id, status_code):
    reason = f"http_error_{status_code}" if status_code else "http_error"
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into visualizacao_resolvidos.audit_ticket_watch
                (ticket_id, table_name, hit_count, last_reason, last_seen_at)
            values (%s, 'tickets_resolvidos', 1, %s, now())
            on conflict (ticket_id, table_name) do update
                set hit_count   = visualizacao_resolvidos.audit_ticket_watch.hit_count + 1,
                    last_reason = excluded.last_reason,
                    last_seen_at = now()
            returning hit_count
            """,
            (ticket_id, reason),
        )
        row = cur.fetchone()
        hit_count = row[0] if row else None
        if hit_count is not None and hit_count >= MAX_HTTP_ERROR_RETRIES:
            cur.execute(
                """
                delete from visualizacao_resolvidos.audit_recent_missing
                where table_name = 'tickets_resolvidos'
                  and ticket_id = %s
                """,
                (ticket_id,),
            )
    conn.commit()


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    conn = psycopg2.connect(NEON_DSN)
    try:
        missing_ids = get_missing_tickets(conn, REPROCESS_LIMIT)
        if not missing_ids:
            print("Nenhum ticket missing para reprocessar.")
            return

        print(f"Reprocessando {len(missing_ids)} tickets: {missing_ids}")

        rows = []
        processed_ids = []

        for tid in missing_ids:
            try:
                ticket = fetch_ticket(tid)
            except requests.HTTPError as e:
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                register_http_error(conn, tid, status_code)
                time.sleep(THROTTLE)
                continue
            except Exception:
                register_http_error(conn, tid, None)
                time.sleep(THROTTLE)
                continue

            if ticket:
                row = map_row(ticket)
                rows.append(row)
                processed_ids.append(tid)

            time.sleep(THROTTLE)

        n_upsert = upsert_rows(conn, rows)
        n_del = delete_missing(conn, processed_ids)

        print(f"UPSERT: {n_upsert} | REMOVIDOS DE audit_recent_missing: {n_del}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
