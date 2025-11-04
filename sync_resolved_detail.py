import os, time, json, requests, psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

NEON_DSN = os.getenv("NEON_DSN")
MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN")
SCHEMA = "visualizacao_resolvidos"
CONCURRENCY = int(os.getenv("CONCURRENCY","8"))
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT","1000"))

def pg():
    return psycopg2.connect(NEON_DSN)

def ensure():
    conn = pg(); cur = conn.cursor()
    cur.execute(f"create schema if not exists {SCHEMA}")
    cur.execute(f"""
        create table if not exists {SCHEMA}.tickets_resolvidos(
          ticket_id bigint primary key,
          status text not null,
          last_resolved_at timestamptz,
          last_closed_at timestamptz,
          last_update timestamptz not null,
          responsible_id bigint,
          responsible_name text,
          organization_id text,
          organization_name text,
          updated_at timestamptz not null default now()
        )
    """)
    cur.execute(f"create index if not exists ix_tr_last_update on {SCHEMA}.tickets_resolvidos(last_update)")
    cur.execute(f"create table if not exists {SCHEMA}.detail_control(ticket_id bigint primary key, last_update timestamptz not null)")
    cur.execute(f"create table if not exists {SCHEMA}.sync_control(name text primary key, last_update timestamptz not null, last_index_run_at timestamptz, last_resolvidos_run_at timestamptz)")
    cur.execute(f"insert into {SCHEMA}.sync_control(name,last_update) values('tr_lastresolved', timestamp '1970-01-01') on conflict (name) do nothing")
    conn.commit(); cur.close(); conn.close()

def load_candidates():
    sql = f"""
      select d.ticket_id, d.last_update
      from {SCHEMA}.detail_control d
      left join {SCHEMA}.tickets_resolvidos r on r.ticket_id = d.ticket_id
      where r.ticket_id is null or d.last_update > r.last_update
      order by d.last_update asc
      limit %s
    """
    conn = pg(); cur = conn.cursor()
    cur.execute(sql, (BATCH_LIMIT,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def to_dt(s):
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except:
        return None

def get_resp(obj):
    rb = obj.get("resolvedBy") or {}
    if isinstance(rb, dict) and (rb.get("id") or rb.get("businessName")):
        return rb.get("id"), rb.get("businessName")
    ow = obj.get("owner") or {}
    return ow.get("id"), ow.get("businessName") or ow.get("fullName")

def get_org(obj):
    clients = obj.get("clients") or []
    for c in clients:
        oid = c.get("organizationId") or c.get("organizationID") or c.get("organization_id")
        onm = c.get("organizationName") or c.get("organization_name")
        if oid or onm:
            return str(oid) if oid is not None else None, onm
    org = obj.get("organization") or {}
    return str(org.get("id")) if org.get("id") is not None else None, org.get("name")

def scan_actions(obj):
    last_resolved = None
    last_closed = None
    actions = obj.get("actions") or []
    for a in actions:
        st = a.get("newStatus") or a.get("status") or ""
        ts = a.get("date") or a.get("createdDate") or a.get("created")
        dt = to_dt(ts)
        if not dt:
            continue
        if st in ("Resolvido","Resolvido e Fechado","Resolvido/Fechado"):
            if not last_resolved or dt > last_resolved:
                last_resolved = dt
        if st in ("Fechado","Cancelado"):
            if not last_closed or dt > last_closed:
                last_closed = dt
    return last_resolved, last_closed

def fetch_detail(tid):
    base = "https://api.movidesk.com/public/v1"
    params = {"token": MOVIDESK_TOKEN, "$expand": "clients,owner,resolvedBy,actions"}
    for path in (f"/tickets/{tid}", f"/tickets/past/{tid}"):
        r = requests.get(base + path, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (404,410):
            continue
        time.sleep(1)
    return None

def upsert_row(tid, detail, ctrl_lu):
    status = detail.get("status") or ""
    last_update = to_dt(detail.get("lastUpdate")) or ctrl_lu
    rid, rname = get_resp(detail)
    oid, oname = get_org(detail)
    rdt, cdt = scan_actions(detail)
    conn = pg(); cur = conn.cursor()
    cur.execute(
        f"""
        insert into {SCHEMA}.tickets_resolvidos
        (ticket_id,status,last_resolved_at,last_closed_at,last_update,responsible_id,responsible_name,organization_id,organization_name,updated_at)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
        on conflict (ticket_id) do update set
          status=excluded.status,
          last_resolved_at=coalesce(excluded.last_resolved_at,{SCHEMA}.tickets_resolvidos.last_resolved_at),
          last_closed_at=coalesce(excluded.last_closed_at,{SCHEMA}.tickets_resolvidos.last_closed_at),
          last_update=excluded.last_update,
          responsible_id=excluded.responsible_id,
          responsible_name=excluded.responsible_name,
          organization_id=excluded.organization_id,
          organization_name=excluded.organization_name,
          updated_at=now()
        """,
        (tid, status, rdt, cdt, last_update, rid, rname, oid, oname)
    )
    conn.commit(); cur.close(); conn.close()

def run_batch(cands):
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        for tid, lu in cands:
            ex.submit(process_one, tid, lu)

def process_one(tid, lu):
    d = fetch_detail(tid)
    if d:
        upsert_row(tid, d, lu)

def mark_run():
    conn = pg(); cur = conn.cursor()
    cur.execute(f"update {SCHEMA}.sync_control set last_resolvidos_run_at=now() where name='tr_lastresolved'")
    conn.commit(); cur.close(); conn.close()

def main():
    ensure()
    cands = load_candidates()
    if not cands:
        mark_run()
        return
    run_batch(cands)
    mark_run()

if __name__ == "__main__":
    main()
