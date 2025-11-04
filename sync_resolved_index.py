import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            wait = int(r.headers.get("retry-after") or 60); time.sleep(wait); continue
        if r.status_code == 404: return []
        r.raise_for_status(); return r.json() if r.text else []

def iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")

def get_since(conn):
    overlap_min = int(os.getenv("MOVIDESK_OVERLAP_MIN","60"))
    days_back = int(os.getenv("MOVIDESK_INDEX_DAYS","180"))
    floor = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_back)
    with conn.cursor() as cur:
        cur.execute("select last_index_run_at from visualizacao_resolvidos.sync_control order by last_index_run_at desc nulls last limit 1")
        row = cur.fetchone()
    if row and row[0]:
        return max(row[0] - datetime.timedelta(minutes=overlap_min), floor)
    return floor

def fetch_index(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    skip = 0
    select_fields = "id,lastUpdate"
    filtro = "(" + " or ".join(["baseStatus eq 'Resolved'","baseStatus eq 'Closed'","baseStatus eq 'Canceled'"]) + f") and lastUpdate ge {since_iso}"
    out=[]
    while True:
        page = _req(url, {"token":API_TOKEN,"$select":select_fields,"$filter":filtro,"$top":top,"$skip":skip}) or []
        if not page: break
        out.extend(page)
        if len(page) < top: break
        skip += len(page); time.sleep(throttle)
    return out

def upsert_detail_control(conn, items):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,
            "insert into visualizacao_resolvidos.detail_control (ticket_id,last_update) values (%s,%s) on conflict (ticket_id) do update set last_update=excluded.last_update",
            [(int(i["id"]), i.get("lastUpdate")) for i in items if str(i.get("id","")).isdigit()],
            page_size=300
        )
        cur.execute("update visualizacao_resolvidos.sync_control set last_index_run_at = now()")
        if cur.rowcount == 0:
            cur.execute("insert into visualizacao_resolvidos.sync_control (last_index_run_at) values (now())")
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    conn = psycopg2.connect(NEON_DSN)
    with conn.cursor() as cur:
        cur.execute("set time zone 'UTC'")
    conn.commit()
    try:
        since = iso_z(get_since(conn))
        items = fetch_index(since)
        if items:
            upsert_detail_control(conn, items)
        print(f"INDEX: {len(items)} tickets")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
