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

def _iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_ids_by_field(window_from, window_to, field_name, page_size, throttle):
    url = f"{API_BASE}/tickets"
    skip = 0
    select_fields = f"id,{field_name}"
    filtro = f"({field_name} ge {_iso_z(window_from)} and {field_name} le {_iso_z(window_to)})"
    out = {}
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$filter": filtro,
            "$orderby": f"{field_name} asc",
            "$top": page_size,
            "$skip": skip
        }) or []
        if not page:
            break
        for t in page:
            tid = t.get("id")
            if not str(tid).isdigit():
                continue
            dt = t.get(field_name)
            if not dt:
                continue
            tid = int(tid)
            if field_name == "resolvedIn":
                out[tid] = (dt, "resolved")
            else:
                if tid not in out:
                    out[tid] = (dt, "closed")
        if len(page) < page_size:
            break
        skip += len(page)
        time.sleep(throttle)
    return out

def ensure_ddl():
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
        begin;
        create schema if not exists visualizacao_resolvidos;

        create table if not exists visualizacao_resolvidos.audit_recent_run (
          run_id bigserial primary key
        );
        do $$
        begin
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='run_at') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column run_at timestamptz not null default now()';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='window_from') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column window_from timestamptz not null default now()';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='window_to') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column window_to timestamptz not null default now()';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='total_api') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column total_api integer not null default 0';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='total_local') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column total_local integer not null default 0';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='missing_total') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column missing_total integer not null default 0';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_run' and column_name='notes') then
            execute 'alter table visualizacao_resolvidos.audit_recent_run add column notes text';
          end if;
        end $$;

        create table if not exists visualizacao_resolvidos.audit_recent_missing (
          run_id bigint not null,
          table_name text not null,
          ticket_id integer not null
        );
        do $$
        begin
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_missing' and column_name='event_in') then
            execute 'alter table visualizacao_resolvidos.audit_recent_missing add column event_in timestamptz';
          end if;
          if not exists (select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='audit_recent_missing' and column_name='event_type') then
            execute 'alter table visualizacao_resolvidos.audit_recent_missing add column event_type text';
          end if;
        end $$;

        truncate table visualizacao_resolvidos.audit_recent_missing;

        do $$
        begin
          if not exists (
            select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where c.relname='ix_audit_missing_run' and n.nspname='visualizacao_resolvidos'
          ) then
            execute 'create index ix_audit_missing_run on visualizacao_resolvidos.audit_recent_missing(run_id)';
          end if;
          if not exists (
            select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where c.relname='ix_audit_missing_ticket' and n.nspname='visualizacao_resolvidos'
          ) then
            execute 'create index ix_audit_missing_ticket on visualizacao_resolvidos.audit_recent_missing(ticket_id)';
          end if;
        end $$;
        commit;
        """)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    ensure_ddl()

    now = _utc_now()
    window_to = now
    window_from = now - datetime.timedelta(days=2)

    page_size = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

    resolved_map = fetch_ids_by_field(window_from, window_to, "resolvedIn", page_size, throttle)
    closed_map   = fetch_ids_by_field(window_from, window_to, "closedIn",   page_size, throttle)

    api_map = dict(closed_map)
    api_map.update(resolved_map)
    api_ids = set(api_map.keys())

    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute("select ticket_id from visualizacao_resolvidos.tickets_resolvidos")
        local_tickets = {r[0] for r in cur.fetchall()}
        cur.execute("select ticket_id from visualizacao_resolvidos.resolvidos_acoes")
        local_acoes = {r[0] for r in cur.fetchall()}
        cur.execute("select ticket_id from visualizacao_resolvidos.detail_control")
        local_detail = {r[0] for r in cur.fetchall()}
        local_union = local_tickets | local_acoes | local_detail

        cur.execute("""
            insert into visualizacao_resolvidos.audit_recent_run
            (run_at,window_from,window_to,total_api,total_local,missing_total,notes)
            values (now(), %s, %s, %s, 0, 0, null) returning run_id
        """, (window_from, window_to, len(api_ids)))
        run_id = cur.fetchone()[0]

        missing_rows = []
        for tid in api_ids:
            event_in, event_type = api_map.get
