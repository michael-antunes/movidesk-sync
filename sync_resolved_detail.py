import os, time, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

def get_conn():
    return psycopg2.connect(
        NEON_DSN,
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if str(ra).isdigit() else 60
            time.sleep(wait); continue
        if r.status_code == 404: return []
        r.raise_for_status()
        return r.json() if r.text else []

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("create table if not exists visualizacao_resolvidos.tickets_resolvidos(ticket_id integer primary key)")
        cur.execute("""
        alter table visualizacao_resolvidos.tickets_resolvidos
          add column if not exists status text,
          add column if not exists last_resolved_at timestamptz,
          add column if not exists last_closed_at timestamptz,
          add column if not exists last_cancelled_at timestamptz,
          add column if not exists last_update timestamptz,
          add column if not exists responsible_id bigint,
          add column if not exists responsible_name text,
          add column if not exists organization_id text,
          add column if not exists organization_name text,
          add column if not exists origin text,
          add column if not exists category text,
          add column if not exists urgency text,
          add column if not exists service_first_level text,
          add column if not exists service_second_level text,
          add column if not exists service_third_level text,
          add column if not exists adicional_137641_avaliado_csat text
        """)
        cur.execute("""
        alter table visualizacao_resolvidos.tickets_resolvidos
          add column if not exists last_resolved_date_br_date date generated always as (((last_resolved_at at time zone 'America/Sao_Paulo')::date)) stored,
          add column if not exists last_resolved_date_br_str  text generated always as (to_char((last_resolved_at at time zone 'America/Sao_Paulo'),'DD/MM/YYYY')) stored,
          add column if not exists last_closed_date_br_date   date generated always as (((last_closed_at  at time zone 'America/Sao_Paulo')::date)) stored,
          add column if not exists last_closed_date_br_str    text generated always as (to_char((last_closed_at  at time zone 'America/Sao_Paulo'),'DD/MM/YYYY')) stored,
          add column if not exists last_cancelled_date_br_date date generated always as (((last_cancelled_at at time zone 'America/Sao_Paulo')::date)) stored,
          add column if not exists last_cancelled_date_br_str  text generated always as (to_char((last_cancelled_at at time zone 'America/Sao_Paulo'),'DD/MM/YYYY')) stored
        """)
        cur.execute("create table if not exists visualizacao_resolvidos.detail_control(ticket_id integer primary key)")
        cur.execute("""
        alter table visualizacao_resolvidos.detail_control
          add column if not exists status text,
          add column if not exists last_resolved_at timestamptz,
          add column if not exists last_closed_at timestamptz,
          add column if not exists last_cancelled_at timestamptz,
          add column if not exists last_update timestamptz,
          add column if not exists need_detail boolean default true,
          add column if not exists detail_last_run_at timestamptz,
          add column if not exists tries integer default 0
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz not null default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)
        cur.execute("create index if not exists ix_dc_need on visualizacao_resolvidos.detail_control(need_detail,last_update)")
        cur.execute("create index if not exists ix_tr_status on visualizacao_resolvidos.tickets_resolvidos(status)")
    conn.commit()

def get_since(conn):
    with conn.cursor() as cur:
        cur.execute("select last_detail_run_at from visualizacao_resolvidos.sync_control where name='default'")
        r1 = cur.fetchone()
        cur.execute("select last_index_run_at from visualizacao_resolvidos.sync_control where name='default'")
        r2 = cur.fetchone()
    if r1 and r1[0]:
        base = r1[0]
    elif r2 and r2[0]:
        base = r2[0]
    else:
        base = datetime.now(timezone.utc) - timedelta(days=int(os.getenv("MOVIDESK_DETAIL_DAYS","2")))
    overlap_min = int(os.getenv("MOVIDESK_OVERLAP_MIN","30"))
    return base - timedelta(minutes=overlap_min)

def iint(x):
    try:
        s = str(x); return int(s) if s.isdigit() else None
    except: return None

def pick_org(clients):
    if not isinstance(clients, list) or not clients: return None, None
    c0 = clients[0] or {}; org = c0.get("organization") or {}
    return org.get("id"), org.get("businessName")

def extract_csat(cfs):
    if not isinstance(cfs, list): return None
    for cf in cfs:
        if str(cf.get("id")) == "137641":
            return cf.get("value")
    return None

def fetch_pages(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE","200"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.8"))
    filtro = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso}"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand_options = [
        "owner,clients($expand=organization),customFields",
        "owner,clients($expand=organization)",
        "owner,clients",
        ""
    ]
    items, skip, exp_idx = [], 0, 0
    while True:
        try:
            params = {"token":API_TOKEN,"$select":select_fields,"$filter":filtro,"$top":top,"$skip":skip}
            if expand_options[exp_idx]: params["$expand"]=expand_options[exp_idx]
            page = req(url, params) or []
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400 and exp_idx < len(expand_options)-1:
                exp_idx += 1; continue
            raise
        if not isinstance(page, list) or not page: break
        items.extend(page)
        if len(page) < top: break
        skip += len(page)
        time.sleep(throttle)
    return items

UPSERT = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 responsible_id,responsible_name,organization_id,organization_name,origin,category,urgency,
 service_first_level,service_second_level,service_third_level,adicional_137641_avaliado_csat)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s,%(origin)s,%(category)s,%(urgency)s,
 %(service_first_level)s,%(service_second_level)s,%(service_third_level)s,%(adicional_137641_avaliado_csat)s)
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
  service_third_level=excluded.service_third_level,
  adicional_137641_avaliado_csat=excluded.adicional_137641_avaliado_csat
"""

def map_row(t):
    owner = t.get("owner") or {}
    org_id, org_name = pick_org(t.get("clients") or [])
    csat = extract_csat(t.get("customFields") or [])
    return {
        "ticket_id": iint(t.get("id")),
        "status": t.get("status"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at": t.get("closedIn"),
        "last_cancelled_at": t.get("canceledIn"),
        "last_update": t.get("lastUpdate"),
        "responsible_id": iint(owner.get("id")),
        "responsible_name": owner.get("businessName"),
        "organization_id": org_id,
        "organization_name": org_name,
        "origin": "" if t.get("origin") is None else str(t.get("origin")),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "adicional_137641_avaliado_csat": csat,
    }

def upsert_rows(conn, rows):
    if not rows: return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=int(os.getenv("MOVIDESK_PG_PAGESIZE","300")))
    conn.commit(); return len(rows)

def mark_done(conn, ids):
    if not ids: return
    with conn.cursor() as cur:
        cur.execute("""
        update visualizacao_resolvidos.detail_control
           set need_detail=false, detail_last_run_at=now(), tries=0
         where ticket_id = any(%s)
        """, (ids,))
    conn.commit()

def up_sync_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
          insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
          values('default',now(),now())
          on conflict (name) do update set last_update=excluded.last_update,last_detail_run_at=excluded.last_detail_run_at
        """)
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    c = get_conn()
    try:
        ensure_schema(c)
        since = get_since(c)
    finally:
        c.close()
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    data = fetch_pages(since_iso)
    rows = [map_row(t) for t in data if isinstance(t, dict)]
    rows = [r for r in rows if r["ticket_id"]]
    ids = [r["ticket_id"] for r in rows]
    c = get_conn()
    try:
        upsert_rows(c, rows)
        mark_done(c, ids)
        up_sync_rows(c)
    finally:
        c.close()
    print(f"pages_upsert={len(rows)} since={since_iso}")

if __name__ == "__main__":
    main()
