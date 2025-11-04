import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            wait = int(r.headers.get("retry-after") or 60); time.sleep(wait); continue
        if r.status_code == 404: return []
        r.raise_for_status(); return r.json() if r.text else []

def iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")

def get_since_utc(conn):
    overlap_min = int(os.getenv("MOVIDESK_OVERLAP_MIN","120"))
    days_back = int(os.getenv("MOVIDESK_INDEX_DAYS","180"))
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    floor = now_utc - datetime.timedelta(days=days_back)
    with conn.cursor() as cur:
        cur.execute("select max(last_index_run_at) from visualizacao_resolvidos.sync_control")
        last_index = cur.fetchone()[0]
        cur.execute("select max(last_update) from visualizacao_resolvidos.detail_control")
        max_detail = cur.fetchone()[0]
    cands = []
    if last_index: cands.append(last_index - datetime.timedelta(minutes=overlap_min))
    if max_detail: cands.append(max_detail - datetime.timedelta(minutes=overlap_min))
    since = min(cands) if cands else floor
    return max(since, floor)

def fetch_index(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    skip = 0
    select_fields = "id,lastUpdate"
    status_filter = " or ".join([
        "baseStatus eq 'Resolved'","baseStatus eq 'Closed'","baseStatus eq 'Canceled'",
        "baseStatus eq 'Resolvido'","baseStatus eq 'Fechado'","baseStatus eq 'Cancelado'"
    ])
    filtro = f"({status_filter}) and lastUpdate ge {since_iso}"
    out=[]
    while True:
        page = _req(url, {"token":API_TOKEN,"$select":select_fields,"$filter":filtro,"$top":top,"$skip":skip}) or []
        if not page: break
        out.extend(page)
        if len(page) < top: break
        skip += len(page); time.sleep(throttle)
    return out

def upsert_detail_control(conn, items):
    if not items: return 0
    rows = [(int(i["id"]), i.get("lastUpdate")) for i in items if str(i.get("id","")).isdigit()]
    if not rows: return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,
            "insert into visualizacao_resolvidos.detail_control (ticket_id,last_update) values (%s,%s) on conflict (ticket_id) do update set last_update=excluded.last_update",
            rows, page_size=500
        )
        cur.execute("update visualizacao_resolvidos.sync_control set last_index_run_at = now()")
        if cur.rowcount == 0:
            cur.execute("insert into visualizacao_resolvidos.sync_control (last_index_run_at) values (now())")
    conn.commit()
    return len(rows)

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    conn = psycopg2.connect(NEON_DSN)
    with conn.cursor() as cur: cur.execute("set time zone 'UTC'")
    conn.commit()
    try:
        since_iso = iso_z(get_since_utc(conn))
        items = fetch_index(since_iso)
        n = upsert_detail_control(conn, items)
        print(f"INDEX rows={n} since={since_iso}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
