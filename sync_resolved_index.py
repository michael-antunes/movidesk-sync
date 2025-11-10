import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def req(url, params, timeout=90):
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

def ensure_ctl(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("""
        create table if not exists visualizacao_resolvidos.detail_control(
          ticket_id integer primary key,
          status text not null,
          last_resolved_at timestamptz,
          last_closed_at timestamptz,
          last_cancelled_at timestamptz,
          last_update timestamptz,
          need_detail boolean not null default true,
          detail_last_run_at timestamptz,
          tries integer not null default 0
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key default 'default',
          last_update timestamptz not null default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)
        cur.execute("create index if not exists ix_dc_need on visualizacao_resolvidos.detail_control(need_detail,last_update)")
    conn.commit()

def get_since(conn):
    days = int(os.getenv("MOVIDESK_INDEX_DAYS","180"))
    with conn.cursor() as cur:
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
    select_fields = "id,status,resolvedIn,closedIn,canceledIn,lastUpdate"
    items = []
    while True:
        page = req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$filter": filtro,
            "$top": top,
            "$skip": skip
        }) or []
        if not page:
            break
        items.extend(page)
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return items

UPSERT = """
insert into visualizacao_resolvidos.detail_control
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,need_detail,tries)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,true,0)
on conflict (ticket_id) do update set
  status=excluded.status,
  last_resolved_at=excluded.last_resolved_at,
  last_closed_at=excluded.last_closed_at,
  last_cancelled_at=excluded.last_cancelled_at,
  need_detail = case when excluded.last_update is distinct from visualizacao_resolvidos.detail_control.last_update
                        or excluded.status is distinct from visualizacao_resolvidos.detail_control.status
                     then true else visualizacao_resolvidos.detail_control.need_detail end,
  last_update=excluded.last_update,
  tries = case when excluded.last_update is distinct from visualizacao_resolvidos.detail_control.last_update then 0 else visualizacao_resolvidos.detail_control.tries end
"""

def map_row(t):
    tid = str(t.get("id"))
    return {
        "ticket_id": int(tid) if tid.isdigit() else None,
        "status": t.get("status"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at": t.get("closedIn"),
        "last_cancelled_at": t.get("canceledIn"),
        "last_update": t.get("lastUpdate"),
    }

def upsert(conn, rows):
    rows = [r for r in rows if r["ticket_id"] is not None]
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=500)
    conn.commit()
    return len(rows)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    conn = psycopg2.connect(NEON_DSN)
    try:
        ensure_ctl(conn)
        since_dt = get_since(conn)
        items = fetch_index(iso_z(since_dt))
        rows = [map_row(t) for t in items if isinstance(t, dict)]
        n = upsert(conn, rows)
        with conn.cursor() as cur:
            cur.execute("""
              insert into visualizacao_resolvidos.sync_control(name,last_update,last_index_run_at)
              values('default',now(),now())
              on conflict (name) do update set last_update=excluded.last_update,last_index_run_at=excluded.last_index_run_at
            """)
        conn.commit()
        print(f"rows={n}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
