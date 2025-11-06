import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE  = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            wait = int(r.headers.get("retry-after") or 60)
            time.sleep(wait); continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def _iso_z(dt: datetime.datetime) -> str:
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc)\
             .isoformat().replace("+00:00","Z")

def _parse_ts(s: str | None):
    if not s: return None
    try:
        return datetime.datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(datetime.timezone.utc)
    except Exception:
        return None

def fetch_ids_resolved_only(since_dt: datetime.datetime) -> list[int]:
    url = f"{API_BASE}/tickets"
    top, throttle, skip = 500, 0.25, 0
    # usamos lastUpdate para paginar; o filtro final é por resolvedIn >= since_dt
    filtro = ("(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') "
              f"and lastUpdate ge {_iso_z(since_dt - datetime.timedelta(days=1))}")
    select_fields = "id,resolvedIn,lastUpdate"

    ids = set()
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
            "$top": top,
            "$skip": skip
        }) or []
        if not isinstance(page, list) or not page:
            break
        for t in page:
            tid = t.get("id")
            if not str(tid).isdigit(): 
                continue
            rid = _parse_ts(t.get("resolvedIn"))
            if rid and rid >= since_dt:
                ids.add(int(tid))
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return sorted(ids)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")

    now = datetime.datetime.now(datetime.timezone.utc)
    since_dt = now - datetime.timedelta(days=2)
    ids = fetch_ids_resolved_only(since_dt)

    with psycopg2.connect(NEON_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            create table if not exists visualizacao_resolvidos.audit_recent_run(
              id bigserial primary key,
              started_at timestamptz not null default now(),
              window_start timestamptz not null,
              window_end   timestamptz not null,
              total_api    integer not null,
              missing_total integer not null
            );
            """)
            cur.execute("""
            create table if not exists visualizacao_resolvidos.audit_recent_missing(
              run_id    bigint not null references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
              table_name text not null,
              ticket_id  integer not null
            );
            """)

        tables_to_check = ("tickets_resolvidos","resolvidos_acoes","detail_control")
        missing_rows: list[tuple[str,int]] = []
        for tname in tables_to_check:
            with conn.cursor() as cur:
                cur.execute(
                    f"select ticket_id from visualizacao_resolvidos.{tname} where ticket_id = any(%s)",
                    (ids,)
                )
                present = {r[0] for r in cur.fetchall()}
            for tid in ids:
                if tid not in present:
                    missing_rows.append((tname, tid))

        with conn.cursor() as cur:
            # registra execução
            cur.execute(
                "insert into visualizacao_resolvidos.audit_recent_run(window_start,window_end,total_api,missing_total) "
                "values(%s,%s,%s,%s) returning id",
                (since_dt, now, len(ids), len(missing_rows))
            )
            run_id = cur.fetchone()[0]

            # mantém SOMENTE a última execução em audit_recent_missing
            cur.execute("truncate table visualizacao_resolvidos.audit_recent_missing")

            if missing_rows:
                psycopg2.extras.execute_batch(
                    cur,
                    "insert into visualizacao_resolvidos.audit_recent_missing(run_id,table_name,ticket_id) "
                    "values (%s,%s,%s)",
                    [(run_id,t,tid) for t,tid in missing_rows],
                    page_size=500
                )
        conn.commit()

if __name__ == "__main__":
    main()
