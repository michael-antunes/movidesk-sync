import os
import time
import logging
import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
LOOPS = int(os.getenv("LOOPS", "200000"))
THROTTLE = float(os.getenv("THROTTLE", "0.05"))

TOP = int(os.getenv("TOP", "500"))
TIMEOUT = int(os.getenv("TIMEOUT", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def conn():
    return psycopg2.connect(DSN)


def api_get(path, params):
    url = f"{API_BASE}/{path}"
    params = dict(params or {})
    params["token"] = TOKEN
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def api_list_ids(path, id_start, id_end):
    ids = set()
    skip = 0
    while True:
        data = api_get(
            path,
            {
                "$select": "id",
                "$filter": f"id ge {int(id_start)} and id le {int(id_end)}",
                "$top": TOP,
                "$skip": skip,
            },
        )
        if isinstance(data, dict) and "items" in data:
            data = data["items"]
        if not isinstance(data, list):
            break
        for row in data:
            if isinstance(row, dict) and "id" in row:
                ids.add(int(row["id"]))
        if len(data) < TOP:
            break
        skip += TOP
        time.sleep(THROTTLE)
    return ids


def table_cols(cur, schema, table):
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema=%s and table_name=%s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def ensure_missing_unique(cur):
    try:
        cur.execute(
            f"""
            create unique index if not exists audit_recent_missing_table_ticket_uniq
            on {SCHEMA}.audit_recent_missing (table_name, ticket_id)
            """
        )
    except Exception:
        pass


def ensure_run_table(cur):
    cur.execute(
        f"""
        create table if not exists {SCHEMA}.audit_recent_runs (
            run_id bigint primary key,
            created_at timestamptz default now(),
            api_total bigint default 0,
            inserted_total bigint default 0,
            range_total bigint default 0
        )
        """
    )


def insert_run(cur, run_id):
    ensure_run_table(cur)
    cur.execute(
        f"""
        insert into {SCHEMA}.audit_recent_runs (run_id)
        values (%s)
        on conflict (run_id) do nothing
        """,
        (int(run_id),),
    )


def update_run(cur, run_id, api_total, inserted_total, range_total):
    ensure_run_table(cur)
    cur.execute(
        f"""
        update {SCHEMA}.audit_recent_runs
        set api_total=%s, inserted_total=%s, range_total=%s
        where run_id=%s
        """,
        (int(api_total), int(inserted_total), int(range_total), int(run_id)),
    )


def get_scan_cursor(cur):
    cur.execute(f"select coalesce(max(ticket_id), 0) from {SCHEMA}.tickets_resolvidos_detail")
    id_inicial = int(cur.fetchone()[0] or 0)

    cur.execute("select coalesce(min(ticket_id), 0) from visualizacao_atual.tickets_abertos")
    id_abertos_min = int(cur.fetchone()[0] or 0)

    cur.execute(f"select coalesce(max(ticket_id), 0) from {SCHEMA}.tickets_mesclados")
    id_mesclados_max = int(cur.fetchone()[0] or 0)

    id_final = min(x for x in [id_inicial, id_abertos_min] if x > 0) if (id_inicial > 0 or id_abertos_min > 0) else 0
    cursor = max(id_inicial, id_mesclados_max)
    return id_final, id_inicial, cursor


def upsert_missing(cur, run_id, table_name, missing_ids):
    cols = table_cols(cur, SCHEMA, "audit_recent_missing")
    base_cols = ["table_name", "ticket_id"]
    base_vals = ["%s", "%s"]

    if "run_id" in cols:
        base_cols.append("run_id")
        base_vals.append("%s")

    if "first_seen" in cols:
        base_cols.append("first_seen")
        base_vals.append("now()")

    if "last_seen" in cols:
        base_cols.append("last_seen")
        base_vals.append("now()")

    if "attempts" in cols:
        base_cols.append("attempts")
        base_vals.append("1")

    if "created_at" in cols:
        base_cols.append("created_at")
        base_vals.append("now()")

    if "updated_at" in cols:
        base_cols.append("updated_at")
        base_vals.append("now()")

    values = []
    for tid in sorted(missing_ids):
        row_params = [table_name, int(tid)]
        if "run_id" in cols:
            row_params.append(int(run_id))
        values.append(tuple(row_params))

    if not values:
        return 0

    conflict = ""
    if "table_name" in cols and "ticket_id" in cols:
        sets = []
        if "last_seen" in cols:
            sets.append("last_seen=now()")
        if "updated_at" in cols:
            sets.append("updated_at=now()")
        if "attempts" in cols:
            sets.append("attempts=attempts+1")
        conflict = f" on conflict (table_name, ticket_id) do update set {', '.join(sets)}" if sets else " on conflict (table_name, ticket_id) do nothing"

    sql = f"insert into {SCHEMA}.audit_recent_missing ({', '.join(base_cols)}) values %s{conflict}"
    template = f"({', '.join(base_vals)})"
    psycopg2.extras.execute_values(cur, sql, values, template=template, page_size=500)
    return len(values)


def main():
    run_id = int(time.time())
    logging.info("run_id=%s table_name=%s", run_id, TABLE_NAME)

    with conn() as c:
        with c.cursor() as cur:
            ensure_missing_unique(cur)
            insert_run(cur, run_id)
            c.commit()

        api_total = 0
        inserted_total = 0
        range_total = 0

        with c.cursor() as cur:
            id_final, id_inicial, cursor = get_scan_cursor(cur)

        for _ in range(LOOPS):
            if cursor < id_final:
                break

            high_id = cursor
            low_id = max(id_final, cursor - BATCH_SIZE + 1)
            range_total += (high_id - low_id + 1)

            api_ids = set()
            api_ids |= api_list_ids("tickets", low_id, high_id)
            api_ids |= api_list_ids("tickets/past", low_id, high_id)
            api_total += len(api_ids)

            with conn() as c2:
                with c2.cursor() as cur2:
                    cur2.execute(
                        f"select ticket_id from {SCHEMA}.tickets_resolvidos_detail where ticket_id between %s and %s",
                        (low_id, high_id),
                    )
                    in_detail = {int(r[0]) for r in cur2.fetchall()}

                    cur2.execute(
                        "select ticket_id from visualizacao_atual.tickets_abertos where ticket_id between %s and %s",
                        (low_id, high_id),
                    )
                    in_abertos = {int(r[0]) for r in cur2.fetchall()}

                    cur2.execute(
                        f"select ticket_id from {SCHEMA}.tickets_mesclados where ticket_id between %s and %s",
                        (low_id, high_id),
                    )
                    in_mesclados = {int(r[0]) for r in cur2.fetchall()}

                    known = set()
                    known |= api_ids
                    known |= in_detail
                    known |= in_abertos
                    known |= in_mesclados

                    missing = set(range(low_id, high_id + 1)) - known

                    inserted = upsert_missing(cur2, run_id, TABLE_NAME, missing)
                    inserted_total += inserted
                    c2.commit()

            cursor = low_id - 1

            with conn() as c3:
                with c3.cursor() as cur3:
                    update_run(cur3, run_id, api_total, inserted_total, range_total)
                    c3.commit()

    logging.info("done api_total=%s inserted_total=%s range_total=%s", api_total, inserted_total, range_total)


if __name__ == "__main__":
    main()
