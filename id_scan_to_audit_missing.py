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
LOOPS = int(os.getenv("LOOPS", "2"))
TOP = int(os.getenv("TOP", "1000"))
THROTTLE = float(os.getenv("THROTTLE", "0.20"))

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
    try:
        cur.execute(
            f"""
            create unique index if not exists ux_audit_recent_missing_uniq
            on {SCHEMA}.audit_recent_missing (table_name, ticket_id)
            """
        )
    except Exception:
        pass


def insert_run(cur, run_id):
    cols = table_cols(cur, SCHEMA, "audit_recent_run")
    ins_cols = []
    ins_vals = []
    params = []

    def add_expr(col, expr):
        if col in cols:
            ins_cols.append(col)
            ins_vals.append(expr)

    def add_param(col, val):
        if col in cols:
            ins_cols.append(col)
            ins_vals.append("%s")
            params.append(val)

    add_param("run_id", run_id)
    add_expr("started_at", "now()")
    add_expr("window_start", "now()")
    add_expr("window_end", "now()")
    add_expr("run_at", "now()")
    add_expr("total_api", "0")
    add_expr("missing_total", "0")
    add_expr("api_ids", "0")
    add_expr("inserted_missing", "0")
    add_expr("range_count", "0")
    add_param("table_name", TABLE_NAME)
    add_param("notes", "id-scan")

    if not ins_cols:
        return

    sql = f"insert into {SCHEMA}.audit_recent_run ({', '.join(ins_cols)}) values ({', '.join(ins_vals)})"
    cur.execute(sql, params)


def update_run(cur, run_id, api_ids, inserted, range_count):
    cols = table_cols(cur, SCHEMA, "audit_recent_run")
    sets = []
    params = []

    def set_expr(col, expr):
        if col in cols:
            sets.append(f"{col}={expr}")

    def set_param(col, val):
        if col in cols:
            sets.append(f"{col}=%s")
            params.append(val)

    set_expr("finished_at", "now()")
    set_param("api_ids", api_ids)
    set_param("inserted_missing", inserted)
    set_param("range_count", range_count)
    set_param("missing_total", inserted)
    set_param("total_api", api_ids)

    if "run_id" not in cols or not sets:
        return

    params.append(run_id)
    cur.execute(
        f"update {SCHEMA}.audit_recent_run set {', '.join(sets)} where run_id=%s",
        params,
    )


def get_scan_cursor(cur):
    cur.execute(f"select id_final, id_inicial, id_atual from {SCHEMA}.range_scan_control limit 1")
    id_final, id_inicial, id_atual = cur.fetchone()
    if id_atual is None:
        id_atual = id_inicial
    return int(id_final), int(id_inicial), int(id_atual)


def set_scan_cursor(cur, new_cursor):
    cur.execute(f"update {SCHEMA}.range_scan_control set id_atual=%s", (int(new_cursor),))


def api_list_ids(endpoint, low_id, high_id):
    ids = set()
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "$select": "id",
            "$filter": f"id ge {low_id} and id le {high_id}",
            "$orderby": "id asc",
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

                    missing = {i for i in api_ids if i not in in_detail and i not in in_abertos and i not in in_mesclados}

                    inserted = upsert_missing(cur2, run_id, TABLE_NAME, missing)
                    inserted_total += inserted

                    set_scan_cursor(cur2, low_id - 1)
                    c2.commit()

            logging.info("range=%s..%s api=%s missing_upsert=%s cursor->%s", low_id, high_id, len(api_ids), inserted, low_id - 1)
            cursor = low_id - 1

        with conn() as c3:
            with c3.cursor() as cur3:
                update_run(cur3, run_id, api_total, inserted_total, range_total)
                c3.commit()

    logging.info("done api_total=%s inserted_total=%s range_total=%s", api_total, inserted_total, range_total)


if __name__ == "__main__":
    main()
