import os, time, requests, psycopg2, psycopg2.extras

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
          name text primary key default 'default',
          last_update timestamptz not null default now()
        )
        """)
        cur.execute("alter table visualizacao_resolvidos.sync_control add column if not exists last_index_run_at timestamptz")
        cur.execute("alter table visualizacao_resolvidos.sync_control add column if not exists last_detail_run_at timestamptz")
        cur.execute("create index if not exists ix_dc_need on visualizacao_resolvidos.detail_control(need_detail,last_update)")
        cur.execute("create index if not exists ix_tr_status on visualizacao_resolvidos.tickets_resolvidos(status)")
    conn.commit()

def pick_ids(limit):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.detail_control
             where need_detail = true or detail_last_run_at is null
             order by coalesce(last_update, now()) desc
             limit %s
            """, (limit,))
            rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def iint(x):
    try:
        s = str(x); return int(s) if s.isdigit() else None
    except: return None

def pick_org(clients):
    if not isinstance(clients, list) or not clients: return None, None
    c0 = clients[0] or {}; org = c0.get("organization") or {}
    return org.get("id"), org.get("businessName")

def slug(s):
    import re
    return re.sub(r"[^a-z0-9]+","_",str(s or "").lower()).strip("_")

def extract_custom(cfs):
    out = {}
    if isinstance(cfs, list):
        for cf in cfs:
            fid = cf.get("id"); name = cf.get("name")
            key = f"adicional_{fid}_{slug(name)}" if fid else None
            if key: out[key] = cf.get("value")
    return out

def fetch_detail(ticket_id):
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand_opts = [
        "owner,clients($expand=organization),customFields",
        "owner,clients($expand=organization)",
        "owner,clients",
        ""
    ]
    url_id = f"{API_BASE}/tickets/{ticket_id}"
    for exp in expand_opts:
        params = {"token":API_TOKEN, "$select":select_fields}
        if exp: params["$expand"] = exp
        try:
            data = req(url_id, params) or {}
            if isinstance(data, dict) and data.get("id"): return data
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400: continue
            raise
    url_list = f"{API_BASE}/tickets"
    for exp in expand_opts:
        params = {"token":API_TOKEN, "$select":select_fields, "$filter": f"id eq {ticket_id}", "$top": 1}
        if exp: params["$expand"] = exp
        try:
            data = req(url_list, params) or []
            if isinstance(data, list) and data: return data[0]
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400: continue
            raise
    return {}

UPSERT_TK = """
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
    cfs = extract_custom(t.get("customFields") or [])
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
        "adicional_137641_avaliado_csat": cfs.get("adicional_137641_avaliado_csat"),
    }

def upsert_detail(conn, rows):
    if not rows: return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_TK, rows, page_size=int(os.getenv("MOVIDESK_PG_PAGESIZE","100")))
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

def safe_flush(rows, done):
    if not rows: return
    for attempt in (1,2):
        conn = None
        try:
            conn = get_conn()
            upsert_detail(conn, rows)
            mark_done(conn, done)
            with conn.cursor() as cur:
                cur.execute("""
                  insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
                  values('default',now(),now())
                  on conflict (name) do update set last_update=excluded.last_update,last_detail_run_at=excluded.last_detail_run_at
                """)
            conn.commit()
            return
        except psycopg2.OperationalError:
            if conn:
                try: conn.close()
                except: pass
            if attempt == 2: raise
            time.sleep(3)
        finally:
            if conn:
                try: conn.close()
                except: pass

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    first = get_conn()
    try:
        ensure_schema(first)
    finally:
        first.close()
    limit = int(os.getenv("MOVIDESK_DETAIL_LIMIT","3000"))
    chunk = int(os.getenv("MOVIDESK_DETAIL_CHUNK","250"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.20"))
    ids = pick_ids(limit)
    rows, done = [], []
    for tid in ids:
        t = fetch_detail(tid)
        if t:
            rows.append(map_row(t)); done.append(tid)
        if len(rows) >= chunk:
            safe_flush(rows, done)
            rows, done = [], []
        time.sleep(throttle)
    if rows:
        safe_flush(rows, done)
    print("ok")

if __name__ == "__main__":
    main()
