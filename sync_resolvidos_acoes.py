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
        if r.status_code == 404: return {}
        r.raise_for_status()
        return r.json() if r.text else {}

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("""
        create table if not exists visualizacao_resolvidos.resolvidos_acoes(
          ticket_id integer primary key,
          acoes jsonb
        )
        """)
        cur.execute("""
        do $$
        begin
          if not exists(
            select 1 from information_schema.columns
            where table_schema='visualizacao_resolvidos'
              and table_name='resolvidos_acoes'
              and column_name='qtd_acoes_descricao_publi'
              and is_generated='ALWAYS'
          ) then
            begin
              alter table visualizacao_resolvidos.resolvidos_acoes
                drop column if exists qtd_acoes_descricao_publi;
            exception when undefined_column then null;
            end;
            alter table visualizacao_resolvidos.resolvidos_acoes
              add column qtd_acoes_descricao_publi integer
              generated always as (
                jsonb_array_length(
                  jsonb_path_query_array(
                    acoes,
                    '$[*] ? (@.description != "" && ((@.isPublic == true) || (@.isPublic == "true")))'
                  )
                )
              ) stored;
          end if;

          if not exists(
            select 1 from information_schema.columns
            where table_schema='visualizacao_resolvidos'
              and table_name='resolvidos_acoes'
              and column_name='qtd_acoes_descricao_inter'
              and is_generated='ALWAYS'
          ) then
            begin
              alter table visualizacao_resolvidos.resolvidos_acoes
                drop column if exists qtd_acoes_descricao_inter;
            exception when undefined_column then null;
            end;
            alter table visualizacao_resolvidos.resolvidos_acoes
              add column qtd_acoes_descricao_inter integer
              generated always as (
                jsonb_array_length(
                  jsonb_path_query_array(
                    acoes,
                    '$[*] ? (@.description != "" && ((@.isPublic == false) || (@.isPublic == "false")))'
                  )
                )
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

def list_ids(conn, since_dt, limit, repair_days):
    with conn.cursor() as cur:
        cur.execute("""
        with delta as (
          select ticket_id, last_update
            from visualizacao_resolvidos.tickets_resolvidos
           where last_update >= %s
        ),
        missing as (
          select tr.ticket_id, tr.last_update
            from visualizacao_resolvidos.tickets_resolvidos tr
            left join visualizacao_resolvidos.resolvidos_acoes ra
              on ra.ticket_id = tr.ticket_id
           where (ra.ticket_id is null or ra.acoes is null or jsonb_array_length(coalesce(ra.acoes,'[]'::jsonb))=0)
             and tr.last_update >= now() - (%s || ' days')::interval
        )
        select ticket_id
          from (select * from delta union all select * from missing) q
         order by last_update desc
         limit %s
        """, (since_dt, repair_days, limit))
        rows = cur.fetchall()
    return [r[0] for r in rows]

def fetch_actions(ticket_id):
    select_fields = "id,status,lastUpdate"
    expand_opts = [
        "actions($select=id,isPublic,description,createdDate,origin)",
        "actions($select=id,isPublic,description,createdDate)"
    ]
    url = f"{API_BASE}/tickets/{ticket_id}"
    for exp in expand_opts:
        try:
            data = req(url, {"token":API_TOKEN, "$select":select_fields, "$expand":exp}) or {}
            acts = data.get("actions") or []
            if isinstance(acts, list): return acts
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400: continue
            raise
    url2 = f"{API_BASE}/tickets"
    for exp in expand_opts:
        try:
            data = req(url2, {"token":API_TOKEN, "$select":select_fields, "$filter":f"id eq {ticket_id}", "$expand":exp, "$top":1}) or {}
            if isinstance(data, list) and data:
                acts = data[0].get("actions") or []
                if isinstance(acts, list): return acts
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400: continue
            raise
    url3 = f"{API_BASE}/tickets/{ticket_id}/actions"
    try:
        acts = req(url3, {"token":API_TOKEN, "$select":"id,isPublic,description,createdDate,origin"})
        if isinstance(acts, list): return acts
    except:
        pass
    return []

def simplify_actions(actions):
    out = []
    for a in actions:
        out.append({
            "id": a.get("id"),
            "isPublic": a.get("isPublic"),
            "description": a.get("description"),
            "createdDate": a.get("createdDate"),
            "origin": a.get("origin")
        })
    return out

UPSERT = """
insert into visualizacao_resolvidos.resolvidos_acoes
(ticket_id,acoes)
values (%(ticket_id)s,%(acoes)s)
on conflict (ticket_id) do update set
  acoes = excluded.acoes
"""

def flush_rows(conn, rows, page):
    if not rows: return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=page)
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
    limit = int(os.getenv("MOVIDESK_ACOES_LIMIT","5000"))
    chunk = int(os.getenv("MOVIDESK_ACOES_CHUNK","150"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.2"))
    repair_days = os.getenv("MOVIDESK_ACOES_REPAIR_DAYS","30")
    c1 = get_conn()
    ids = list_ids(c1, since_dt, limit, repair_days)
    c1.close()
    rows = []
    for tid in ids:
        acts = fetch_actions(tid)
        simp = simplify_actions(acts)
        rows.append({"ticket_id": tid, "acoes": psycopg2.extras.Json(simp)})
        if len(rows) >= chunk:
            c = get_conn(); flush_rows(c, rows, int(os.getenv("MOVIDESK_PG_PAGESIZE","200"))); heartbeat(c); c.close(); rows = []
        time.sleep(throttle)
    if rows:
        c = get_conn(); flush_rows(c, rows, int(os.getenv("MOVIDESK_PG_PAGESIZE","200"))); heartbeat(c); c.close()
    print("ok")

if __name__ == "__main__":
    main()
