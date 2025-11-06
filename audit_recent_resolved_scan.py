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

def _iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

def fetch_recent_ids(since_iso):
    url = f"{API_BASE}/tickets"
    top = 500
    throttle = 0.25
    skip = 0
    select_fields = "id,lastUpdate"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge " + since_iso
    ids = []
    while True:
        page = _req(url, {"token": API_TOKEN, "$select": select_fields, "$filter": filtro, "$orderby": "lastUpdate asc", "$top": top, "$skip": skip}) or []
        if not isinstance(page, list) or not page:
            break
        for i in page:
            tid = i.get("id")
            if str(tid).isdigit():
                ids.append(int(tid))
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return sorted(set(ids))

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("missing envs")
    now = datetime.datetime.now(datetime.timezone.utc)
    start = now - datetime.timedelta(days=2)
    ids = fetch_recent_ids(_iso_z(start))
    with psycopg2.connect(NEON_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            create table if not exists visualizacao_resolvidos.audit_recent_run(
              id bigserial primary key,
              started_at timestamptz not null default now(),
              window_start timestamptz not null,
              window_end timestamptz not null,
              total_api integer not null,
              missing_total integer not null
            )""")
            cur.execute("""
            create table if not exists visualizacao_resolvidos.audit_recent_missing(
              run_id bigint not null references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
              table_name text not null,
              ticket_id integer not null
            )""")
        missing_rows = []
        tables = ["tickets_resolvidos","resolvidos_acoes","detail_control"]
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(f"select ticket_id from visualizacao_resolvidos.{t} where ticket_id = any(%s)", (ids,))
                present = {r[0] for r in cur.fetchall()}
                for tid in ids:
                    if tid not in present:
                        missing_rows.append((t, tid))
        with conn.cursor() as cur:
            cur.execute("insert into visualizacao_resolvidos.audit_recent_run(window_start,window_end,total_api,missing_total) values(%s,%s,%s,%s) returning id", (start, now, len(ids), len(missing_rows)))
            run_id = cur.fetchone()[0]
            if missing_rows:
                psycopg2.extras.execute_batch(cur, "insert into visualizacao_resolvidos.audit_recent_missing(run_id,table_name,ticket_id) values (%s,%s,%s)", [(run_id,t,tid) for t,tid in missing_rows], page_size=500)
        conn.commit()

if __name__ == "__main__":
    main()
