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
        s = str(x); return int(s)
