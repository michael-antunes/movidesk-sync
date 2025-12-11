# -*- coding: utf-8 -*-
import os
import time
import logging
import datetime as dt

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
WINDOW_HOURS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))


def od_get(path, params):
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
            "$orderby": "id desc",
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


def main():
    now = dt.datetime.now(dt.timezone.utc)
    win_end = now
    win_start = now - dt.timedelta(hours=WINDOW_HOURS)

    f_start = iso_odata_z(win_start)
    f_end = iso_odata_z(win_end)

    resolved = f"(resolvedIn ge {f_start} and resolvedIn le {f_end})"
    closed = f"(closedIn ge {f_start} and closedIn le {f_end})"
    odata_filter = f"({resolved} or {closed})"
    select_fields = "id,baseStatus,resolvedIn,closedIn"

    logging.info("Janela: %s -> %s", f_start, f_end)

    ids = set()
    for page in page_iter(odata_filter, select_fields):
        for row in page:
            try:
                ids.add(int(row["id"]))
            except Exception:
                pass

    logging.info("Total API (únicos): %d", len(ids))

    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at,
               window_from, window_to, total_local, notes)
            values (%s, %s, %s, 0, now(), %s, %s,
                    (select count(*) from visualizacao_resolvidos.tickets_resolvidos
                       where coalesce(last_resolved_at,last_closed_at) between %s and %s),
                    'scan resolved+closed')
            returning id
            """,
            (win_start, win_end, len(ids), win_start, win_end, win_start, win_end),
        )
        run_id = cur.fetchone()[0]
        missing_total = 0

        cur.execute(
            """
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where coalesce(last_resolved_at,last_closed_at) between %s and %s
            """,
            (win_start, win_end),
        )
        local_ids = {r[0] for r in cur.fetchall()}
        miss_tk = sorted(ids - local_ids)
        if miss_tk:
            execute_values(
                cur,
                """
                insert into visualizacao_resolvidos.audit_recent_missing
                    (run_id, table_name, ticket_id)
                values %s
                on conflict do nothing
                """,
                [(run_id, "tickets_resolvidos", i) for i in miss_tk],
            )
            missing_total += len(miss_tk)

        if ids:
            cur.execute(
                """
                select ticket_id
                  from visualizacao_resolvidos.resolvidos_acoes
                 where ticket_id = any(%s)
                """,
                (list(ids),),
            )
            ja_tem = {r[0] for r in cur.fetchall()}
            miss_acoes = sorted(ids - ja_tem)
            if miss_acoes:
                execute_values(
                    cur,
                    """
                    insert into visualizacao_resolvidos.audit_recent_missing
                        (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                    """,
                    [(run_id, "resolvidos_acoes", i) for i in miss_acoes],
                )
                missing_total += len(miss_acoes)

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
