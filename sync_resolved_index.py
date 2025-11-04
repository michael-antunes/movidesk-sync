import os
import time
import datetime
import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def _as_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def _iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

def _get_since(conn, job_name):
    with conn.cursor() as cur:
        cur.execute("select last_update from visualizacao_resolvidos.sync_control where name=%s", (job_name,))
        row = cur.fetchone()
    base = _as_utc(row[0]) if row and row[0] else None
    if not base:
        days = int(os.getenv("MOVIDESK_INDEX_DAYS", "180"))
        base = _utc_now() - datetime.timedelta(days=days)
    since = base - datetime.timedelta(hours=4)
    return since

def fetch_index(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
    skip = 0
    select_fields = "id,lastUpdate"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge " + since_iso
    items = []
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
            "$top": top,
            "$skip": skip
        }) or []
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return items

def upsert_detail_control(conn, items):
    rows = []
    for i in items:
        tid = i.get("id")
        if str(tid).isdigit():
            rows.append((int(tid), i.get("lastUpdate")))
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,
            """
            insert into visualizacao_resolvidos.detail_control (ticket_id,last_update)
            values (%s,%s)
            on conflict (ticket_id) do update
            set last_update = greatest(excluded.last_update, visualizacao_resolvidos.detail_control.last_update)
            """,
            rows, page_size=500
        )
    conn.commit()
    return len(rows)

def mark_job_end(conn, job_name):
    with conn.cursor() as cur:
        cur.execute("update visualizacao_resolvidos.sync_control set last_update = now() where name=%s", (job_name,))
        if cur.rowcount == 0:
            cur.execute("insert into visualizacao_resolvidos.sync_control(name,last_update) values(%s, now())", (job_name,))
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    with psycopg2.connect(NEON_DSN) as conn:
        since_dt = _get_since(conn, "detail_control")
        since_iso = _iso_z(since_dt)
        items = fetch_index(since_iso)
        upsert_detail_control(conn, items)
        mark_job_end(conn, "detail_control")

if __name__ == "__main__":
    main()
