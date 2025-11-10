import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if str(ra).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def get_since(conn):
    days = int(os.getenv("MOVIDESK_INDEX_DAYS","180"))
    with conn.cursor() as cur:
        cur.execute("""
          create table if not exists visualizacao_resolvidos.sync_control(
            name text primary key default 'default',
            last_update timestamptz not null default now(),
            last_index_run_at timestamptz,
            last_detail_run_at timestamptz
          )
        """)
        conn.commit()
        cur.execute("select last_index_run_at from visualizacao_resolvidos.sync_control where name='default'")
        row = cur.fetchone()
        if row and row[0]:
            return row[0] - datetime.timedelta(minutes=int(os.getenv("MOVIDESK_OVERLAP_MIN","60")))
    return datetime.datetime.utcnow() - datetime.timedelta(days=days)

def fetch_index(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    skip = 0
    filtro = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso}"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"
    items = []
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$expand": expand,
            "$filter": filtro,
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

def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None

def pick_org(clients):
    if not isinstance(clients, list) or not clients:
        return None, None
    c0 = clients[0] or {}
    org = c0.get("organization") or {}
    return org.get("id"), org.get("businessName")

def map_row(t):
    tid = iint(t.get("id"))
    owner = t.get("owner") or {}
    org_id, org_name = pick_org(t.get("clients") or [])
    return {
        "ticket_id": tid,
        "status": t.get("status"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at": t.get("closedIn"),
        "last_cancelled_at": t.get("canceledIn"),
        "last_update": t.get("lastUpdate"),
        "responsible_id": iint(owner.get("id")),
        "responsible_name": owner.get("businessName"),
        "organization_id": org_id,
        "organization_name": org_name,
        "origin": str(t.get("origin") if t.get("origin") is not None else ""),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 responsible_id,responsible_name,organization_id,organization_name,origin,category,urgency,
 service_first_level,service_second_level,service_third_level)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s,%(origin)s,%(category)s,%(urgency)s,
 %(service_first_level)s,%(service_second_level)s,%(service_third_level)s)
on conflict (ticket_id) do update set
  status=excluded.status,
  last_resolved_at=excluded.last_resolved_at,
  last_closed_at=excluded.last_closed_at,
  last_cancelled_at=excluded.last_cancelled_at,
  last_update=excluded.last_update,
  responsible_id=excluded.responsible_id,
  responsible_name=excluded.responsible_name,
  organization_id=excluded.organization_id,
  organization_name=excluded.organization_name,
  origin=excluded.origin,
  category=excluded.category,
  urgency=excluded.urgency,
  service_first_level=excluded.service_first_level,
  service_second_level=excluded.service_second_level,
  service_third_level=excluded.service_third_level
"""

def upsert(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=300)
        conn.commit()
    return len(rows)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    conn = psycopg2.connect(NEON_DSN)
    try:
        since_dt = get_since(conn)
        items = fetch_index(iso_z(since_dt))
        rows = [map_row(t) for t in items if isinstance(t, dict)]
        upsert(conn, rows)
        with conn.cursor() as cur:
            cur.execute("""
              insert into visualizacao_resolvidos.sync_control(name,last_update,last_index_run_at)
              values('default',now(),now())
              on conflict (name) do update set last_update=excluded.last_update,last_index_run_at=excluded.last_index_run_at
            """)
            conn.commit()
        print(f"rows={len(rows)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
