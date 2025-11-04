import os, time, requests, psycopg2
from datetime import datetime, timedelta, timezone

NEON_DSN = os.getenv("NEON_DSN")
MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN")
SCHEMA = "visualizacao_resolvidos"
PAGE_SIZE = int(os.getenv("PAGE_SIZE","1000"))

def pg():
    return psycopg2.connect(NEON_DSN)

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ensure():
    conn = pg(); cur = conn.cursor()
    cur.execute(f"create schema if not exists {SCHEMA}")
    cur.execute(f"create table if not exists {SCHEMA}.detail_control(ticket_id bigint primary key, last_update timestamptz not null)")
    cur.execute(f"create table if not exists {SCHEMA}.sync_control(name text primary key, last_update timestamptz not null, last_index_run_at timestamptz, last_resolvidos_run_at timestamptz)")
    cur.execute(f"insert into {SCHEMA}.sync_control(name,last_update) values('tr_lastresolved', timestamp '1970-01-01') on conflict (name) do nothing")
    conn.commit(); cur.close(); conn.close()

def load_watermark():
    conn = pg(); cur = conn.cursor()
    cur.execute(f"select last_update from {SCHEMA}.sync_control where name='tr_lastresolved'")
    wm, = cur.fetchone(); cur.close(); conn.close()
    return wm

def save_progress(max_lu):
    conn = pg(); cur = conn.cursor()
    cur.execute(f"update {SCHEMA}.sync_control set last_update=%s, last_index_run_at=now() where name='tr_lastresolved'", (max_lu,))
    conn.commit(); cur.close(); conn.close()

def upsert_detail(rows):
    conn = pg(); cur = conn.cursor()
    cur.executemany(
        f"""
        insert into {SCHEMA}.detail_control(ticket_id,last_update)
        values (%s,%s)
        on conflict (ticket_id) do update
        set last_update = greatest({SCHEMA}.detail_control.last_update, excluded.last_update)
        """,
        rows
    )
    conn.commit(); cur.close(); conn.close()

def fetch_page(skip, since_iso):
    url = "https://api.movidesk.com/public/v1/tickets"
    params = {
        "token": MOVIDESK_TOKEN,
        "$select": "id,lastUpdate,status",
        "$filter": f"(status eq \"Resolvido\" or status eq \"Fechado\" or status eq \"Cancelado\") and lastUpdate ge {since_iso}",
        "$orderby": "lastUpdate asc",
        "$top": str(PAGE_SIZE),
        "$skip": str(skip)
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        return []
    return r.json() or []

def main():
    ensure()
    wm = load_watermark()
    since = wm - timedelta(hours=4)
    since_iso = iso_z(since)
    skip = 0
    max_lu = wm
    while True:
        page = fetch_page(skip, since_iso)
        if not page:
            break
        rows = []
        for it in page:
            tid = int(it.get("id") or 0)
            lu_raw = it.get("lastUpdate")
            if not tid or not lu_raw:
                continue
            try:
                lu = datetime.fromisoformat(lu_raw.replace("Z","+00:00"))
            except:
                continue
            rows.append((tid, lu))
            if lu > max_lu:
                max_lu = lu
        if rows:
            upsert_detail(rows)
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.2)
    save_progress(max_lu)

if __name__ == "__main__":
    main()
