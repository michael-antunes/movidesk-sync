import os
import time
import logging
import datetime as dt

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s %(message)s")

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ.get("MOVIDESK_TOKEN")
DSN = os.environ.get("NEON_DSN")
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
WINDOW_HOURS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))

def od_get(path, params):
    if not TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não definido")
    url = f"{BASE}/{path}"
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    return r.json()

def page_iter(filter_expr, select_fields):
    top = PAGE_SIZE
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "$filter": filter_expr,
            "$select": select_fields,
            "$orderby": "lastUpdate desc",
            "$skip": skip,
            "$top": top,
        }
        data = od_get("tickets", params)
        if not data:
            break
        yield data
        if len(data) < top:
            break
        skip += top
        time.sleep(THROTTLE)

def iso_odata_z(d):
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    d = d.astimezone(dt.timezone.utc).replace(microsecond=0)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_api_dt(s):
    if not s:
        return None
    try:
        text = str(s)
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(text).astimezone(dt.timezone.utc)
    except Exception:
        return None

def normalize_dt(d):
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(microsecond=0)

def main():
    if not DSN:
        raise RuntimeError("NEON_DSN não definido")

    now = dt.datetime.now(dt.timezone.utc)
    win_end = now
    win_start = now - dt.timedelta(hours=WINDOW_HOURS)

    f_start = iso_odata_z(win_start)
    f_end = iso_odata_z(win_end)

    base_filter = "(" + " or ".join(
        [
            "baseStatus eq 'Resolved'",
            "baseStatus eq 'Closed'",
            "baseStatus eq 'Canceled'",
        ]
    ) + ")"
    lu_filter = f"(lastUpdate ge {f_start} and lastUpdate le {f_end})"
    odata_filter = f"{base_filter} and {lu_filter}"

    select_fields = "id,baseStatus,lastUpdate"

    logging.info("Janela (lastUpdate): %s -> %s", f_start, f_end)

    api_ids = set()
    api_last_update = {}

    for page in page_iter(odata_filter, select_fields):
        for row in page:
            try:
                tid = int(row.get("id"))
            except Exception:
                continue
            lu_str = row.get("lastUpdate")
            lu_dt = parse_api_dt(lu_str)
            if not lu_dt:
                continue
            if lu_dt < win_start or lu_dt > win_end:
                continue
            api_ids.add(tid)
            api_last_update[tid] = lu_dt

    logging.info("Total API (únicos): %d", len(api_ids))

    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select count(*)
            from visualizacao_resolvidos.tickets_resolvidos_detail
            where last_update between %s and %s
            """,
            (win_start, win_end),
        )
        local_count = cur.fetchone()[0] or 0

        cur.execute(
            """
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at,
               window_from, window_to, total_local, notes)
            values (%s, %s, %s, 0, now(), %s, %s, %s, %s)
            returning id
            """,
            (
                win_start,
                win_end,
                len(api_ids),
                win_start,
                win_end,
                local_count,
                "scan lastUpdate",
            ),
        )
        run_id = cur.fetchone()[0]

        missing_ids = []

        if api_ids:
            cur.execute(
                """
                select ticket_id, last_update
                from visualizacao_resolvidos.tickets_resolvidos_detail
                where ticket_id = any(%s)
                """,
                (list(api_ids),),
            )
            rows = cur.fetchall()
            local_map = {int(r[0]): r[1] for r in rows}

            for tid in api_ids:
                api_dt = normalize_dt(api_last_update.get(tid))
                db_dt = normalize_dt(local_map.get(tid))
                if api_dt is None:
                    continue
                if db_dt is None or api_dt > db_dt:
                    missing_ids.append(tid)

        missing_total = 0

        if missing_ids:
            execute_values(
                cur,
                """
                insert into visualizacao_resolvidos.audit_recent_missing
                  (run_id, table_name, ticket_id)
                values %s
                on conflict do nothing
                """,
                [(run_id, "tickets_resolvidos", i) for i in sorted(missing_ids)],
            )
            missing_total = len(missing_ids)

        cur.execute(
            """
            update visualizacao_resolvidos.audit_recent_run
               set missing_total = %s
             where id = %s
            """,
            (missing_total, run_id),
        )

        conn.commit()

    logging.info("Run %s finalizado. missing_total=%d", run_id, missing_total)

if __name__ == "__main__":
    main()
