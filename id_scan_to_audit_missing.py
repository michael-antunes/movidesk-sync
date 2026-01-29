import os
import time
import logging
import random
import datetime as dt
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

def _parse_dt(v):
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        d = v
    else:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            d = dt.datetime.fromisoformat(s)
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(microsecond=0)

def api_list_meta(path, id_start, id_end):
    meta = {}
    skip = 0
    while True:
        data = api_get(
            path,
            {
                "$select": "id,lastUpdate",
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
            if not isinstance(row, dict):
                continue
            if "id" not in row:
                continue
            try:
                tid = int(row["id"])
            except Exception:
                continue
            meta[tid] = row.get("lastUpdate")
        if len(data) < TOP:
            break
        skip += TOP
        if THROTTLE > 0:
            time.sleep(THROTTLE)
    return meta

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
                    window_start timestamptz not null default now(),
                    window_end timestamptz not null default now(),
                    table_name text not null default '{TABLE_NAME}',
                    notes text,
                    total_api bigint default 0,
                    missing_total bigint default 0,
                    api_ids bigint default 0,
                    inserted_missing bigint default 0,
                    range_count bigint default 0,
                    range_total bigint default 0
                )
                """
            )

def ensure_cursor_table():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(
                f"""
                create table if not exists {SCHEMA}.range_scan_control (
                    data_inicio timestamptz,
                    data_fim timestamptz,
                    ultima_data_validada timestamptz,
                    scan_cursor bigint
                )
                """
            )
            cur.execute(f"select count(*) from {SCHEMA}.range_scan_control")
            if (cur.fetchone() or [0])[0] == 0:
                cur.execute(f"insert into {SCHEMA}.range_scan_control (scan_cursor) values (0)")

def get_scan_cursor(cur):
    cur.execute(f"select coalesce(scan_cursor,0) from {SCHEMA}.range_scan_control limit 1")
    return int((cur.fetchone() or [0])[0])

def set_scan_cursor(cur, cursor_val):
    cur.execute(f"update {SCHEMA}.range_scan_control set scan_cursor=%s", (int(cursor_val),))

def create_run(cur):
    cols = table_cols(cur, SCHEMA, "audit_recent_run")
    insert_cols = ["table_name"]
    insert_vals = ["%s"]
    params = [TABLE_NAME]
    if "notes" in cols:
        insert_cols.append("notes")
        insert_vals.append("%s")
        params.append("id_scan")
    cur.execute(
        f"insert into {SCHEMA}.audit_recent_run ({', '.join(insert_cols)}) values ({', '.join(insert_vals)}) returning id",
        params,
    )
    return int(cur.fetchone()[0])

def update_run(cur, run_pk, total_api, inserted_total, range_total):
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

def upsert_missing(cur, run_pk, table_name, missing_ids):
    cols = table_cols(cur, SCHEMA, "audit_recent_missing")
    base_cols = ["table_name", "ticket_id"]
    base_vals = ["%s", "%s"]

    row_has_run = "run_id" in cols
    if row_has_run:
        base_cols.append("run_id")
        base_vals.append("%s")

    if "run_started_at" in cols:
        base_cols.append("run_started_at")
        base_vals.append("now()")
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
    if "last_seen" in cols or "updated_at" in cols:
        sets = []
        if "last_seen" in cols:
            sets.append("last_seen=now()")
        if "updated_at" in cols:
            sets.append("updated_at=now()")
        if sets:
            conflict = f" on conflict (table_name, ticket_id) do update set {', '.join(sets)}"

    sql = f"insert into {SCHEMA}.audit_recent_missing ({', '.join(base_cols)}) values %s{conflict}"
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
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        ),
    )

def run_db_txn(fn):
    last = None
    for _ in range(TXN_RETRIES):
        c = None
        try:
            c = conn()
            with c:
                with c.cursor() as cur:
                    cur.execute(f"set local lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
                    cur.execute(f"set local statement_timeout = '{STMT_TIMEOUT_MS}ms'")
                    return fn(cur)
        except Exception as e:
            last = e
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass
            if is_retryable(e):
                time.sleep(0.6 + random.random())
                continue
            raise
    raise last

def main():
    if not TOKEN:
        raise SystemExit("MOVIDESK_TOKEN obrigatório")
    if not DSN:
        raise SystemExit("NEON_DSN obrigatório")

    ensure_run_table()
    ensure_cursor_table()
    ensure_missing_unique()

    run_pk = run_db_txn(create_run)
    cursor = run_db_txn(lambda cur: get_scan_cursor(cur))
    if cursor <= 0:
        cursor = 1

    api_total = 0
    inserted_total = 0
    range_total = 0

    loops = 0
    while loops < LOOPS:
        loops += 1
        high_id = cursor
        low_id = max(1, cursor - BATCH_SIZE + 1)
        range_total += (high_id - low_id + 1)

        api_meta = {}
        api_meta.update(api_list_meta("tickets", low_id, high_id))
        api_meta.update(api_list_meta("tickets/past", low_id, high_id))

        api_ids = set(api_meta.keys())
        api_total += len(api_ids)

        def work(cur):
            cur.execute(
                f"select ticket_id, last_update from {SCHEMA}.tickets_resolvidos_detail where ticket_id between %s and %s",
                (low_id, high_id),
            )
            detail_rows = cur.fetchall()
            in_detail = {int(r[0]) for r in detail_rows}
            detail_map = {int(r[0]): r[1] for r in detail_rows}

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

            missing_new = api_ids - local_present

            stale = set()
            for tid in (api_ids & in_detail):
                api_dt = _parse_dt(api_meta.get(tid))
                db_dt = _parse_dt(detail_map.get(tid))
                if api_dt is None:
                    continue
                if db_dt is None or db_dt < api_dt:
                    stale.add(tid)

            missing = set(missing_new) | set(stale)

            inserted = upsert_missing(cur, run_pk, TABLE_NAME, missing)
            set_scan_cursor(cur, low_id - 1)
            return inserted, len(api_ids), low_id - 1, len(missing_new), len(stale), sorted(list(stale))[:30]

        inserted, api_count, new_cursor, c_missing_new, c_stale, stale_sample = run_db_txn(work)
        inserted_total += inserted

        if stale_sample:
            logging.info(
                "range=%s..%s api=%s missing_upsert=%s novos=%s atualizacao=%s stale_ids=%s cursor->%s",
                low_id,
                high_id,
                api_count,
                inserted,
                c_missing_new,
                c_stale,
                ",".join(str(x) for x in stale_sample),
                new_cursor,
            )
        else:
            logging.info(
                "range=%s..%s api=%s missing_upsert=%s novos=%s atualizacao=%s cursor->%s",
                low_id,
                high_id,
                api_count,
                inserted,
                c_missing_new,
                c_stale,
                new_cursor,
            )

        cursor = new_cursor
        if cursor <= 0:
            break

    run_db_txn(lambda cur: update_run(cur, run_pk, api_total, inserted_total, range_total) or None)
    logging.info("done api_total=%s inserted_total=%s range_total=%s", api_total, inserted_total, range_total)
    print(f"done api_total={api_total} inserted_total={inserted_total} range_total={range_total}")

if __name__ == "__main__":
    main()
