import os, time, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

def get_conn():
    return psycopg2.connect(NEON_DSN, sslmode="require", keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def req(url, params, timeout=120):
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
        cur.execute("create table if not exists visualizacao_resolvidos.resolvidos_acoes(ticket_id integer primary key,acoes jsonb)")
        cur.execute("""
        do $$
        begin
          if not exists(select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='resolvidos_acoes' and column_name='qtd_acoes_descricao_publi' and is_generated='ALWAYS') then
            begin alter table visualizacao_resolvidos.resolvidos_acoes drop column if exists qtd_acoes_descricao_publi; exception when undefined_column then null; end;
            alter table visualizacao_resolvidos.resolvidos_acoes add column qtd_acoes_descricao_publi integer generated always as (
              jsonb_array_length(jsonb_path_query_array(acoes,'$[*] ? (@.description != "" && ((@.isPublic == true) || (@.isPublic == "true")))'))
            ) stored;
          end if;
          if not exists(select 1 from information_schema.columns where table_schema='visualizacao_resolvidos' and table_name='resolvidos_acoes' and column_name='qtd_acoes_descricao_inter' and is_generated='ALWAYS') then
            begin alter table visualizacao_resolvidos.resolvidos_acoes drop column if exists qtd_acoes_descricao_inter; exception when undefined_column then null; end;
            alter table visualizacao_resolvidos.resolvidos_acoes add column qtd_acoes_descricao_inter integer generated always as (
              jsonb_array_length(jsonb_path_query_array(acoes,'$[*] ? (@.description != "" && ((@.isPublic == false) || (@.isPublic == "false")))'))
            ) stored;
          end if;
        end$$;
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz not null default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)
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
        base = datetime.now(timezone.utc) - timedelta(days=int(os.getenv("MOVIDESK_ACOES_DAYS","7")))
    return base - timedelta(minutes=int(os.getenv("MOVIDESK_OVERLAP_MIN","15")))

def fetch_bulk(since_iso, top, throttle, hard_days):
    url = f"{API_BASE}/tickets"
    filtro = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso}"
    select_fields = "id,lastUpdate,actions"
    expand = "actions($expand=attachments,timeAppointments;$select=id,isPublic,description,createdDate,origin,attachments,timeAppointments)"
    earliest = (datetime.now(timezone.utc) - timedelta(days=int(hard_days))).strftime("%Y-%m-%dT%H:%M:%SZ")
    filtro2 = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {earliest}"
    items, skip = [], 0
    while True:
        page = req(url, {"token":API_TOKEN,"$select":select_fields,"$expand":expand,"$filter":filtro,"$top":top,"$skip":skip}) or []
        if not page: break
        items.extend(page)
        if len(page) < top: break
        skip += len(page); time.sleep(throttle)
    skip = 0
    while True:
        page = req(url, {"token":API_TOKEN,"$select":select_fields,"$expand":expand,"$filter":filtro2,"$top":top,"$skip":skip}) or []
        if not page: break
        items.extend(page)
        if len(page) < top: break
        skip += len(page); time.sleep(throttle)
    return items

def simplify_action(a):
    atts = a.get("attachments") or []
    tapps = a.get("timeAppointments") or []
    return {
        "id": a.get("id"),
        "isPublic": a.get("isPublic"),
        "description": a.get("description"),
        "createdDate": a.get("createdDate"),
        "origin": a.get("origin"),
        "attachments": [{"id":x.get("id"),"fileName":x.get("fileName"),"link":x.get("link"),"size":x.get("size"),"type":x.get("type") } for x in atts if isinstance(x,dict)],
        "timeAppointments": [{"id":x.get("id"),"workTime":x.get("workTime"),"createdDate":x.get("createdDate"),"time":"%s"%x.get("time")} for x in tapps if isinstance(x,dict)]
    }

def build_rows(tickets):
    rows = []
    for t in tickets:
        tid = t.get("id")
        acts = t.get("actions") or []
        simp = [simplify_action(a) for a in acts if isinstance(a,dict)]
        rows.append({"ticket_id": int(str(tid)) if str(tid).isdigit() else None, "acoes": psycopg2.extras.Json(simp)})
    return [r for r in rows if r["ticket_id"] is not None]

UPSERT = """
insert into visualizacao_resolvidos.resolvidos_acoes(ticket_id,acoes)
values (%(ticket_id)s,%(acoes)s)
on conflict (ticket_id) do update set acoes=excluded.acoes
"""

def upsert_rows(conn, rows, page_size):
    if not rows: return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=page_size)
    conn.commit(); return len(rows)

def heartbeat(conn):
    with conn.cursor() as cur:
        cur.execute("""
        insert into visualizacao_resolvidos.sync_control(name,last_update)
        values('default',now())
        on conflict (name) do update set last_update=excluded.last_update
        """)
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    c0 = get_conn()
    try:
        ensure_schema(c0)
        since_dt = get_since(c0)
    finally:
        c0.close()
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    top = int(os.getenv("MOVIDESK_ACOES_TOP","100"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.3"))
    hard_days = os.getenv("MOVIDESK_ACOES_REPAIR_DAYS","30")
    tickets = fetch_bulk(since_iso, top, throttle, hard_days)
    rows = build_rows(tickets)
    c1 = get_conn()
    try:
        upsert_rows(c1, rows, int(os.getenv("MOVIDESK_PG_PAGESIZE","200")))
        heartbeat(c1)
    finally:
        c1.close()
    print(f"ok rows={len(rows)} since={since_iso}")

if __name__ == "__main__":
    main()
