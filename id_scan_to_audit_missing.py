import os
import time
import logging
import random
import requests
import psycopg2
import psycopg2.extras
from psycopg2 import errors

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = "tickets_resolvidos"

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", os.getenv("BATCH_SIZE", "1000")))
LOOPS = int(os.getenv("ID_SCAN_ITERATIONS", os.getenv("LOOPS", "200000")))
TOP = int(os.getenv("MOVIDESK_TOP", os.getenv("TOP", "500")))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", os.getenv("THROTTLE", "0.05")))
TIMEOUT = int(os.getenv("TIMEOUT", "60"))

LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "5000"))
STMT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "300000"))
UPSERT_PAGE_SIZE = int(os.getenv("DB_UPSERT_PAGE_SIZE", "200"))
TXN_RETRIES = int(os.getenv("DB_TXN_RETRIES", "6"))

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
                try:
                    ids.add(int(row["id"]))
                except Exception:
                    pass
        if len(data) < TOP:
            break
        skip += TOP
        if THROTTLE > 0:
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


def index_exists(cur, schema, index_name):
    cur.execute(
        """
        select 1
        from pg_class c
        join pg_namespace n on n.oid=c.relnamespace
        where n.nspname=%s and c.relkind='i' and c.relname=%s
        """,
        (schema, index_name),
    )
    return cur.fetchone() is not None


def ensure_missing_unique():
    index_name = "ux_audit_recent_missing_table_ticket"
    with conn() as c:
        with c.cursor() as cur:
            if index_exists(cur, SCHEMA, index_name):
                return
    c = conn()
    try:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(
                f"create unique index concurrently {index_name} on {SCHEMA}.audit_recent_missing (table_name, ticket_id)"
            )
    except Exception:
        pass
    finally:
        c.close()


def ensure_run_table():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(
                f"""
                create table if not exists {SCHEMA}.audit_recent_run (
                    id bigserial primary key,
                    started_at timestamptz not null default now(),
                    window_start timestamptz not null,
                    window_end timestamptz not null,
                    total_api integer not null,
                    missing_total integer not null default 0,
                    run_at timestamptz,
                    window_from timestamptz,
                    window_to timestamptz,
                    total_local integer,
                    notes text,
                    run_id bigint,
                    created_at timestamptz,
                    table_name text,
                    external_run_id bigint,
                    workflow text,
                    job text,
                    ref text,
                    sha text,
                    repo text,
                    max_actions integer,
                    window_start_id bigint,
                    window_end_id bigint,
                    window_center_id bigint
                )
                """
            )
            c.commit()


def create_run(external_run_id):
    with conn() as c:
        with c.cursor() as cur:
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

            add_expr("started_at", "now()")
            add_expr("window_start", "now()")
            add_expr("window_end", "now()")
            add_expr("run_at", "now()")
            add_expr("total_api", "0")
            add_expr("missing_total", "0")
            add_expr("created_at", "now()")
            add_param("table_name", TABLE_NAME)
            add_param("notes", "id-scan")
            if "external_run_id" in cols:
                add_param("external_run_id", int(external_run_id))
            elif "run_id" in cols:
                add_param("run_id", int(external_run_id))
            add_param("workflow", os.getenv("GITHUB_WORKFLOW"))
            add_param("job", os.getenv("GITHUB_JOB"))
            add_param("ref", os.getenv("GITHUB_REF_NAME") or os.getenv("GITHUB_REF"))
            add_param("sha", os.getenv("GITHUB_SHA"))
            add_param("repo", os.getenv("GITHUB_REPOSITORY"))

            cur.execute(
                f"insert into {SCHEMA}.audit_recent_run ({', '.join(ins_cols)}) values ({', '.join(ins_vals)}) returning id",
                tuple(params),
            )
            run_pk = int(cur.fetchone()[0])
            c.commit()
            return run_pk


def update_run(run_pk, total_api, inserted_total, range_total):
    with conn() as c:
        with c.cursor() as cur:
            cols = table_cols(cur, SCHEMA, "audit_recent_run")
            sets = []
            params = []

            if "window_end" in cols:
                sets.append("window_end=now()")
            if "total_api" in cols:
                sets.append("total_api=%s")
                params.append(int(total_api))
            if "missing_total" in cols:
                sets.append("missing_total=%s")
                params.append(int(inserted_total))
            if "api_ids" in cols:
                sets.append("api_ids=%s")
                params.append(int(total_api))
            if "inserted_missing" in cols:
                sets.append("inserted_missing=%s")
                params.append(int(inserted_total))
            if "range_count" in cols:
                sets.append("range_count=%s")
                params.append(int(range_total))
            if "range_total" in cols:
                sets.append("range_total=%s")
                params.append(int(range_total))

            if not sets:
                return

            params.append(int(run_pk))
            cur.execute(f"update {SCHEMA}.audit_recent_run set {', '.join(sets)} where id=%s", tuple(params))
            c.commit()


def get_scan_cursor():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(f"select id_final, id_inicial, id_atual from {SCHEMA}.range_scan_control limit 1")
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"{SCHEMA}.range_scan_control sem linhas")
            id_final, id_inicial, id_atual = row
            if id_final is None or id_inicial is None:
                raise RuntimeError(f"{SCHEMA}.range_scan_control precisa de id_inicial e id_final")
            if id_atual is None:
                id_atual = id_inicial
            return int(id_final), int(id_inicial), int(id_atual)


def set_scan_cursor(cur, new_cursor):
    cols = table_cols(cur, SCHEMA, "range_scan_control")
    sets = []
    vals = []
    if "id_atual" in cols:
        sets.append("id_atual=%s")
        vals.append(int(new_cursor))
    if "updated_at" in cols:
        sets.append("updated_at=now()")
    if not sets:
        return
    cur.execute(f"update {SCHEMA}.range_scan_control set {', '.join(sets)}", tuple(vals))


def upsert_missing(cur, run_pk, table_name, missing_ids):
    cols = table_cols(cur, SCHEMA, "audit_recent_missing")
    base_cols = ["table_name", "ticket_id"]
    base_vals = ["%s", "%s"]

    row_has_run = "run_id" in cols
    if row_has_run:
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
        if row_has_run:
            row_params.append(int(run_pk))
        values.append(tuple(row_params))

    if not values:
        return 0

    conflict = " on conflict (table_name, ticket_id) do nothing"
    if "last_seen" in cols or "updated_at" in cols or "attempts" in cols:
        sets = []
        if "last_seen" in cols:
            sets.append("last_seen=now()")
        if "updated_at" in cols:
            sets.append("updated_at=now()")
        if "attempts" in cols:
            sets.append("attempts=arm.attempts+1")
        if sets:
            conflict = f" on conflict (table_name, ticket_id) do update set {', '.join(sets)}"

    sql = f"insert into {SCHEMA}.audit_recent_missing as arm ({', '.join(base_cols)}) values %s{conflict}"
    template = f"({', '.join(base_vals)})"
    psycopg2.extras.execute_values(cur, sql, values, template=template, page_size=UPSERT_PAGE_SIZE)
    return len(values)


def is_retryable(e):
    return isinstance(
        e,
        (
            errors.DeadlockDetected,
            errors.SerializationFailure,
            errors.LockNotAvailable,
            errors.QueryCanceled,
            errors.AdminShutdown,
        ),
    )


def run_db_txn(work, attempts=TXN_RETRIES):
    base_sleep = 0.25
    for i in range(attempts):
        c = conn()
        try:
            with c:
                with c.cursor() as cur:
                    cur.execute(f"set local lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
                    cur.execute(f"set local statement_timeout = '{STMT_TIMEOUT_MS}ms'")
                    return work(cur)
        except Exception as e:
            try:
                c.rollback()
            except Exception:
                pass
            if (i + 1) >= attempts or not is_retryable(e):
                raise
            sleep_s = base_sleep * (2 ** i) + random.random() * 0.25
            time.sleep(sleep_s)
        finally:
            try:
                c.close()
            except Exception:
                pass


def main():
    external_run_id = int(time.time())
    logging.info("external_run_id=%s table_name=%s", external_run_id, TABLE_NAME)

    ensure_run_table()
    ensure_missing_unique()

    run_pk = create_run(external_run_id)

    id_final, id_inicial, cursor = get_scan_cursor()

    api_total = 0
    inserted_total = 0
    range_total = 0

    done = 0

    for _ in range(LOOPS):
        if cursor < id_final:
            done = 1
            break

        high_id = cursor
        low_id = max(id_final, cursor - BATCH_SIZE + 1)
        range_total += (high_id - low_id + 1)

        api_ids = set()
        api_ids |= api_list_ids("tickets", low_id, high_id)
        api_ids |= api_list_ids("tickets/past", low_id, high_id)
        api_total += len(api_ids)

        def work(cur):
            cur.execute(
                f"select ticket_id from {SCHEMA}.tickets_resolvidos_detail where ticket_id between %s and %s",
                (low_id, high_id),
            )
            in_detail = {int(r[0]) for r in cur.fetchall()}

            cur.execute(
                "select ticket_id from visualizacao_atual.tickets_abertos where ticket_id between %s and %s",
                (low_id, high_id),
            )
            in_abertos = {int(r[0]) for r in cur.fetchall()}

            cur.execute(
                f"select ticket_id from {SCHEMA}.tickets_mesclados where ticket_id between %s and %s",
                (low_id, high_id),
            )
            in_mesclados = {int(r[0]) for r in cur.fetchall()}

            local_present = set()
            local_present |= in_detail
            local_present |= in_abertos
            local_present |= in_mesclados

            missing = api_ids - local_present

            inserted = upsert_missing(cur, run_pk, TABLE_NAME, missing)
            set_scan_cursor(cur, low_id - 1)
            return inserted, len(api_ids), low_id - 1

        inserted, api_count, new_cursor = run_db_txn(work)
        inserted_total += inserted

        logging.info(
            "range=%s..%s api=%s missing_upsert=%s cursor->%s",
            low_id,
            high_id,
            api_count,
            inserted,
            new_cursor,
        )
        cursor = new_cursor

    run_db_txn(lambda cur: update_run(run_pk, api_total, inserted_total, range_total) or None)

    logging.info("done api_total=%s inserted_total=%s range_total=%s", api_total, inserted_total, range_total)
    print(f"done={done}")


if __name__ == "__main__":
    main()
