import os, time, requests, psycopg2, psycopg2.extras, datetime

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.environ["MOVIDESK_TOKEN"]
NEON_DSN = os.environ["NEON_DSN"]

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def get_since(conn):
    with conn.cursor() as cur:
        cur.execute("select last_update from visualizacao_resolvidos.sync_control where name='detail_control'")
        row = cur.fetchone()
    base = row[0] if row and row[0] else datetime.datetime.now(datetime.timezone.utc)
    return base - datetime.timedelta(hours=4)

def upsert_rows(conn, rows):
    if not rows:
        return 0
    sql = """
    insert into visualizacao_resolvidos.detail_control(ticket_id,last_update)
    values (%(ticket_id)s,%(last_update)s)
    on conflict (ticket_id) do update
      set last_update = greatest(excluded.last_update, visualizacao_resolvidos.detail_control.last_update)
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)

def update_control(conn):
    with conn.cursor() as cur:
        cur.execute("""
        insert into visualizacao_resolvidos.sync_control(name,last_update)
        values ('detail_control', now())
        on conflict (name) do update set last_update=excluded.last_update
        """)
    conn.commit()

def fetch_changed(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    skip = 0
    select_fields = "id,lastUpdate,baseStatus"
    filtro = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso!r}"
    items = []
    while True:
        page = _req(url, {"token": API_TOKEN, "$select": select_fields, "$filter": filtro, "$top": top, "$skip": skip}) or []
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return items

def to_iso_z(dt):
    return dt.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def main():
    conn = psycopg2.connect(NEON_DSN)
    try:
        since_dt = get_since(conn)
        since_iso = to_iso_z(since_dt)
        items = fetch_changed(since_iso)
        rows = []
        for t in items:
            tid = t.get("id")
            if isinstance(tid,str) and tid.isdigit():
                tid = int(tid)
            lu = t.get("lastUpdate")
            rows.append({"ticket_id": tid, "last_update": lu})
        upsert_rows(conn, rows)
        update_control(conn)
        print(f"indexados: {len(rows)} desde {since_iso}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
