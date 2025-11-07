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
            wait = int(r.headers.get("retry-after") or 60)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json() if r.text else []

def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def _iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc) \
             .isoformat().replace("+00:00", "Z")

def fetch_ids_by_field(window_from, window_to, field_name, page_size, throttle):
    """Retorna {ticket_id: (datetime_iso, 'resolved'|'closed')}"""
    url = f"{API_BASE}/tickets"
    skip = 0
    out = {}
    filtro = f"({field_name} ge {_iso_z(window_from)} and {field_name} le {_iso_z(window_to)})"
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": f"id,{field_name}",
            "$filter": filtro,
            "$orderby": f"{field_name} asc",
            "$top": page_size,
            "$skip": skip,
        }) or []
        if not page:
            break
        for t in page:
            tid = t.get("id")
            if not str(tid).isdigit():
                continue
            when = t.get(field_name)
            if not when:
                continue
            tid = int(tid)
            tag = "resolved" if field_name == "resolvedIn" else "closed"
            # resolved tem prioridade
            if tag == "resolved" or tid not in out:
                out[tid] = (when, tag)
        if len(page) < page_size:
            break
        skip += len(page)
        time.sleep(throttle)
    return out

DDL = """
begin;
create schema if not exists visualizacao_resolvidos;

-- Tabela de runs com SEU layout:
create table if not exists visualizacao_resolvidos.audit_recent_run (
  id bigserial primary key,
  started_at timestamptz not null default now(),
  window_start timestamptz not null,
  window_end   timestamptz not null,
  total_api    integer     not null,
  missing_total integer    not null default 0
);

-- Tabela de faltantes (mantemos run_id -> audit_recent_run.id)
create table if not exists visualizacao_resolvidos.audit_recent_missing (
  run_id    bigint not null,
  table_name text  not null,
  ticket_id integer not null
);

-- Garantir colunas extras (caso já exista sem elas):
do $$
begin
  if not exists (
    select 1 from information_schema.columns
    where table_schema='visualizacao_resolvidos'
      and table_name='audit_recent_missing'
      and column_name='event_in'
  ) then
    execute 'alter table visualizacao_resolvidos.audit_recent_missing add column event_in timestamptz';
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema='visualizacao_resolvidos'
      and table_name='audit_recent_missing'
      and column_name='event_type'
  ) then
    execute 'alter table visualizacao_resolvidos.audit_recent_missing add column event_type text';
  end if;
end $$;

-- manter somente a última execução
truncate table visualizacao_resolvidos.audit_recent_missing;

commit;
"""

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Configure MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    now = _utc_now()
    window_to = now
    window_from = now - datetime.timedelta(days=2)

    page_size = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

    # API: resolvedIn & closedIn
    resolved_map = fetch_ids_by_field(window_from, window_to, "resolvedIn", page_size, throttle)
    closed_map   = fetch_ids_by_field(window_from, window_to, "closedIn",   page_size, throttle)
    api_map = dict(closed_map)
    api_map.update(resolved_map)  # resolved tem prioridade
    api_ids = set(api_map.keys())

    # DDL / garantias
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        # conjuntos locais
        cur.execute("select ticket_id from visualizacao_resolvidos.tickets_resolvidos")
        local_tickets = {r[0] for r in cur.fetchall()}

        cur.execute("select ticket_id from visualizacao_resolvidos.resolvidos_acoes")
        local_acoes = {r[0] for r in cur.fetchall()}

        cur.execute("select ticket_id from visualizacao_resolvidos.detail_control")
        local_detail = {r[0] for r in cur.fetchall()}

        # abre run no seu layout e pega id
        cur.execute("""
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api)
            values (%s, %s, %s)
            returning id
        """, (window_from, window_to, len(api_ids)))
        run_id = cur.fetchone()[0]

        # popula missing por tabela
        missing_rows = []
        for tid in api_ids:
            when, etype = api_map.get(tid)
            if tid not in local_tickets:
                missing_rows.append((run_id, "tickets_resolvidos", tid, when, etype))
            if tid not in local_acoes:
                missing_rows.append((run_id, "resolvidos_acoes", tid, when, etype))
            if tid not in local_detail:
                missing_rows.append((run_id, "detail_control", tid, when, etype))

        if missing_rows:
            psycopg2.extras.execute_batch(
                cur,
                """
                insert into visualizacao_resolvidos.audit_recent_missing
                  (run_id, table_name, ticket_id, event_in, event_type)
                values (%s,%s,%s,%s,%s)
                """,
                missing_rows, page_size=1000
            )

        # atualiza somente missing_total (seu layout)
        cur.execute("""
            update visualizacao_resolvidos.audit_recent_run
               set missing_total = %s
             where id = %s
        """, (len(missing_rows), run_id))

        # índices úteis (idempotentes)
        cur.execute("""
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
        """)
        conn.commit()

if __name__ == "__main__":
    main()
