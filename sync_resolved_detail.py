import os, json, time, concurrent.futures, requests, psycopg2
from datetime import datetime

NEON_DSN = os.getenv("NEON_DSN")
MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN")
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "2000"))
SCHEMA = "visualizacao_resolvidos"

def pg():
    return psycopg2.connect(NEON_DSN)

def ensure_schema():
    conn = pg()
    cur = conn.cursor()
    cur.execute(f"create schema if not exists {SCHEMA}")
    cur.execute(f"create table if not exists {SCHEMA}.detail_control (ticket_id bigint primary key)")
    cur.execute(f"alter table {SCHEMA}.detail_control add column if not exists last_update timestamptz")
    cur.execute(f"alter table {SCHEMA}.detail_control add column if not exists synced_at timestamptz")
    cur.execute(f"create table if not exists {SCHEMA}.tickets_resolvidos (ticket_id bigint primary key)")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists last_update timestamptz")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists detail jsonb")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos add column if not exists updated_at timestamptz default now()")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos alter column updated_at set default now()")
    cur.execute(f"update {SCHEMA}.tickets_resolvidos set detail='{{}}'::jsonb where detail is null")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos alter column detail set default '{{}}'::jsonb")
    cur.execute(f"alter table {SCHEMA}.tickets_resolvidos alter column detail set not null")
    cur.execute(f"create index if not exists ix_tr_last_update on {SCHEMA}.tickets_resolvidos(last_update)")
    cur.execute(f"create index if not exists ix_tr_gin on {SCHEMA}.tickets_resolvidos using gin(detail)")
    conn.commit()
    cur.close()
    conn.close()

def load_pending_ids():
    conn = pg()
    cur = conn.cursor()
    cur.execute(f"""
        select ticket_id, last_update
        from {SCHEMA}.detail_control
        where last_update > coalesce(synced_at, timestamp '1970-01-01')
        order by last_update asc
        limit %s
    """, (BATCH_LIMIT,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {MOVIDESK_TOKEN}"})
    return s

def fetch_detail(sess, tid):
    urls = [
        f"https://api.movidesk.com/public/v1/tickets/{tid}?$expand=clients,owner,actions,createdBy,resolvedBy,team,resolvedIn,customFields",
        f"https://api.movidesk.com/public/v1/tickets/past/{tid}?$expand=clients,owner,actions,createdBy,resolvedBy,team,resolvedIn,customFields"
    ]
    for u in urls:
        r = sess.get(u, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (404, 410):
            continue
        time.sleep(1)
    return None

def upsert_detail(conn, tid, last_update, detail):
    cur = conn.cursor()
    cur.execute(
        f"""
        insert into {SCHEMA}.tickets_resolvidos(ticket_id, last_update, detail, updated_at)
        values (%s, %s, %s::jsonb, now())
        on conflict (ticket_id) do update
        set last_update = excluded.last_update,
            detail = excluded.detail,
            updated_at = now()
        """,
        (tid, last_update, json.dumps(detail))
    )
    cur.execute(
        f"update {SCHEMA}.detail_control set synced_at = %s where ticket_id = %s",
        (last_update, tid)
    )
    conn.commit()
    cur.close()

def worker(args):
    tid, lu = args
    sess = session()
    detail = fetch_detail(sess, tid)
    if detail is None:
        return (tid, False)
    conn = pg()
    upsert_detail(conn, tid, lu, detail)
    conn.close()
    return (tid, True)

def main():
    ensure_schema()
    pending = load_pending_ids()
    if not pending:
        return
    tasks = [(tid, lu) for (tid, lu) in pending]
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        list(ex.map(worker, tasks))

if __name__ == "__main__":
    main()
