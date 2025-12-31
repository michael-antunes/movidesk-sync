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

TOP = int(os.getenv("TOP", os.getenv("MOVIDESK_TOP", "500")))
THROTTLE = float(os.getenv("THROTTLE", os.getenv("MOVIDESK_THROTTLE", "0.05")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def conn():
    return psycopg2.connect(DSN)


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
    cur.execute(
        f"""
        create unique index if not exists audit_recent_missing_table_ticket_uniq
        on {SCHEMA}.audit_recent_missing (table_name, ticket_id)
        """
    )


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


def ensure_cursor_table(cur):
    cur.execute(
        f"""
        create table if not exists {SCHEMA}.range_scan_control (
            table_name text primary key,
            cursor_id bigint,
            updated_at timestamptz default now()
        )
        """
    )


def get_scan_cursor(cur):
    ensure_cursor_table(cur)
    cur.execute(
        f"""
        select cursor_id
        from {SCHEMA}.range_scan_control
        where table_name=%s
        """,
        (TABLE_NAME,),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        return int(row[0])

    cur.execute(f"select coalesce(max(ticket_id), 0) from {SCHEMA}.tickets_resolvidos_detail")
    cursor = int(cur.fetchone()[0] or 0)

    cur.execute(f"select coalesce(max(ticket_id), 0) from {SCHEMA}.tickets_mesclados")
    merged_max = int(cur.fetchone()[0] or 0)

    cursor = max(cursor, merged_max)

    cur.execute(
        f"""
        insert into {SCHEMA}.range_scan_control (table_name, cursor_id)
        values (%s, %s)
        on conflict (table_name) do update set cursor_id=excluded.cursor_id, updated_at=now()
        """,
        (TABLE_NAME, cursor),
    )
    return int(cursor)


def set_scan_cursor(cur, cursor_id):
    ensure_cursor_table(cur)
    cur.execute(
        f"""
        insert into {SCHEMA}.range_scan_control (table_name, cursor_id)
        values (%s, %s)
        on conflict (table_name) do update set cursor_id=excluded.cursor_id, updated_at=now()
        """,
        (TABLE_NAME, int(cursor_id)),
    )


def get_floor_id(cur):
    cur.execute("select coalesce(min(ticket_id), 0) from visualizacao_atual.tickets_abertos")
    abertos_min = int(cur.fetchone()[0] or 0)

    cur.execute(f"select coalesce(min(ticket_id), 0) from {SCHEMA}.tickets_resolvidos_detail")
    detail_min = int(cur.fetchone()[0] or 0)

    candidates = [x for x in (abertos_min, detail_min) if x > 0]
    return min(candidates) if candidates else 0


def movidesk_list_ids(endpoint, low_id, high_id):
    ids = set()
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "$select": "id",
            "$filter": f"id ge {int(low_id)} and id le {int(high_id)}",
            "$top": TOP,
            "$skip": skip,
        }
        r = requests.get(f"{API_BASE}/{endpoint}", params=params, timeout=60)
        if r.status_code == 400:
            raise RuntimeError(f"Movidesk 400 em {endpoint}: {r.text[:400]}")
        r.raise_for_status()
        data = r.json()
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
            sets.append("attempts=arm.attempts+1")
        conflict = (
            f" on conflict (table_name, ticket_id) do update set {', '.join(sets)}"
            if sets
            else " on conflict (table_name, ticket_id) do nothing"
        )

    sql = f"insert into {SCHEMA}.audit_recent_missing as arm ({', '.join(base_cols)}) values %s{conflict}"
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
            cursor = get_scan_cursor(cur)
            floor_id = get_floor_id(cur)

        done = 0

        for _ in range(LOOPS):
            if cursor <= 0:
                done = 1
                break
            if floor_id > 0 and cursor < floor_id:
                done = 1
                break

            high_id = cursor
            low_id = max(1, cursor - BATCH_SIZE + 1)
            if floor_id > 0:
                low_id = max(low_id, floor_id)

            range_total += (high_id - low_id + 1)

            api_ids = set()
            api_ids |= movidesk_list_ids("tickets", low_id, high_id)
            api_ids |= movidesk_list_ids("tickets/past", low_id, high_id)
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

                    missing = {i for i in api_ids if i not in in_detail and i not in in_abertos and i not in in_mesclados}

                    inserted = upsert_missing(cur2, run_id, TABLE_NAME, missing)
                    inserted_total += inserted

                    set_scan_cursor(cur2, low_id - 1)
                    c2.commit()

            logging.info(
                "range=%s..%s api=%s missing_upsert=%s cursor->%s",
                low_id,
                high_id,
                len(api_ids),
                inserted,
                low_id - 1,
            )
            cursor = low_id - 1

        with conn() as c3:
            with c3.cursor() as cur3:
                update_run(cur3, run_id, api_total, inserted_total, range_total)
                c3.commit()

    logging.info("done api_total=%s inserted_total=%s range_total=%s done=%s", api_total, inserted_total, range_total, done)
    print(f"done={done}")


if __name__ == "__main__":
    main()
