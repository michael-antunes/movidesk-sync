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
          acoes jsonb,
          qtd_acoes_descricao_publi integer,
          qtd_acoes_descricao_inter integer
        )
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
    overlap_min = int(os.getenv("MOVIDESK_OVERLAP_MIN","15"))
    return base - timedelta(minutes=overlap_min)

def list_ids(conn, since_dt, limit):
    with conn.cursor() as cur:
        cur.execute("""
        select ticket_id
          from visualizacao_resolvidos.tickets_resolvidos
         where last_update >= %s
         order by last_update desc
         limit %s
        """, (since_dt, limit))
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

def count_descriptions(actions):
    pub = 0
    inter = 0
    for a in actions:
        desc = a.get("description")
        is_pub = bool(a.get("isPublic"))
        if isinstance(desc, str) and desc.strip():
            if is_pub: pub += 1
            else: inter += 1
    return pub, inter

UPSERT = """
insert into visualizacao_resolvidos.resolvidos_acoes
(ticket_id,acoes,qtd_acoes_descricao_publi,qtd_acoes_descricao_inter)
values (%(ticket_id)s,%(acoes)s,%(pub)s,%(inter)s)
on conflict (ticket_id) do update set
  acoes = excluded.acoes,
  qtd_acoes_descricao_publi = excluded.qtd_acoes_descricao_publi,
  qtd_acoes_descricao_inter = excluded.qtd_acoes_descricao_inter
"""

def flush_rows(conn, rows):
    if not rows: return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=int(os.getenv("MOVIDESK_PG_PAGESIZE","200")))
    conn.commit()

def heartbeat(conn):
    with conn.cursor() as cur:
        cur.execute("""
        insert into visualizacao_resolvidos.sync_control(name,last_update)
        values('default',now())
        on conflict (name) do update set last_update=excluded.last_update
        """)
    conn.commit()

def main():
    c0 = get_conn()
    try:
        ensure_schema(c0)
        since_dt = get_since(c0)
    finally:
        c0.close()
    limit = int(os.getenv("MOVIDESK_ACOES_LIMIT","5000"))
    chunk = int(os.getenv("MOVIDESK_ACOES_CHUNK","150"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.2"))
    conn_ids = get_conn()
    ids = list_ids(conn_ids, since_dt, limit)
    conn_ids.close()
    rows = []
    for tid in ids:
        acts = fetch_actions(tid)
        simp = simplify_actions(acts)
        pub, inter = count_descriptions(simp)
        rows.append({"ticket_id": tid, "acoes": psycopg2.extras.Json(simp), "pub": pub, "inter": inter})
        if len(rows) >= chunk:
            c = get_conn(); flush_rows(c, rows); heartbeat(c); c.close(); rows = []
        time.sleep(throttle)
    if rows:
        c = get_conn(); flush_rows(c, rows); heartbeat(c); c.close()
    print("ok")

if __name__ == "__main__":
    main()
