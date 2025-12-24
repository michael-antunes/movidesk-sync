import os
import time
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import execute_values


DSN = os.getenv("NEON_DSN")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")

API_BASE = "https://api.movidesk.com/public/v1"

TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "20"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "60"))
MAX_RUNTIME_SEC = int(os.getenv("ID_SCAN_MAX_RUNTIME_SEC", "240"))

BASE_STATUSES = ("Resolved", "Closed", "Canceled")
SELECT_LIST = "id,baseStatus"

SCHEMA = "visualizacao_resolvidos"
CONTROL_TABLE = f"{SCHEMA}.range_scan_control"


if not DSN:
    raise RuntimeError("NEON_DSN não definido")
if not TOKEN:
    raise RuntimeError("MOVIDESK_TOKEN não definido")


def pg_all(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()


def pg_one(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchone()


def get_table_cols(cur, schema: str, table: str):
    rows = pg_all(
        cur,
        """
        select column_name, data_type, is_nullable, column_default
        from information_schema.columns
        where table_schema=%s and table_name=%s
        """,
        (schema, table),
    )
    d = {}
    for name, dtype, is_nullable, default in rows:
        d[name] = {"dtype": dtype, "notnull": (is_nullable == "NO"), "default": default}
    return d


def ensure_audit_tables(cur):
    cur.execute(f"create schema if not exists {SCHEMA}")
    cur.execute(f"create table if not exists {SCHEMA}.audit_recent_run(id bigserial primary key)")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists window_start timestamptz")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists window_end timestamptz")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists total_api int")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists missing_total int")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists total_local int")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists notes text")
    cur.execute(f"alter table {SCHEMA}.audit_recent_run add column if not exists run_at timestamptz")

    cur.execute(
        f"""
        create table if not exists {SCHEMA}.audit_recent_missing(
          run_id bigint,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )
    cur.execute(
        f"""
        create unique index if not exists audit_recent_missing_uniq
        on {SCHEMA}.audit_recent_missing(table_name, ticket_id)
        """
    )


def insert_run(cur, notes: str):
    cols = get_table_cols(cur, SCHEMA, "audit_recent_run")

    fields = []
    values = []
    params = []

    def add_now(col):
        fields.append(col)
        values.append("now()")

    def add_param(col, v):
        fields.append(col)
        values.append("%s")
        params.append(v)

    required = [c for c, meta in cols.items() if meta["notnull"] and meta["default"] is None and c != "id"]

    for c in required:
        dtype = cols[c]["dtype"]
        lc = c.lower()
        if lc in ("window_start", "window_end", "run_at", "started_at", "created_at", "updated_at"):
            add_now(c)
        elif lc in ("total_api", "missing_total", "total_local"):
            add_param(c, 0)
        elif lc == "notes":
            add_param(c, notes)
        elif dtype in ("timestamp with time zone", "timestamp without time zone", "date"):
            add_now(c)
        elif dtype in ("integer", "bigint", "numeric", "double precision", "real", "smallint"):
            add_param(c, 0)
        elif dtype in ("boolean",):
            add_param(c, False)
        else:
            add_param(c, "")

    if "window_start" in cols and "window_start" not in fields:
        add_now("window_start")
    if "window_end" in cols and "window_end" not in fields:
        add_now("window_end")
    if "run_at" in cols and "run_at" not in fields:
        add_now("run_at")
    if "total_api" in cols and "total_api" not in fields:
        add_param("total_api", 0)
    if "missing_total" in cols and "missing_total" not in fields:
        add_param("missing_total", 0)
    if "total_local" in cols and "total_local" not in fields:
        add_param("total_local", 0)
    if "notes" in cols and "notes" not in fields:
        add_param("notes", notes)

    if not fields:
        cur.execute(f"insert into {SCHEMA}.audit_recent_run default values returning id")
        return int(cur.fetchone()[0])

    cur.execute(
        f"""
        insert into {SCHEMA}.audit_recent_run({",".join(fields)})
        values ({",".join(values)})
        returning id
        """,
        params,
    )
    return int(cur.fetchone()[0])


def update_run(cur, run_id: int, total_api: int, missing_total: int, total_local: int):
    cols = get_table_cols(cur, SCHEMA, "audit_recent_run")
    sets = []
    params = []

    if "window_end" in cols:
        sets.append("window_end=now()")
    if "total_api" in cols:
        sets.append("total_api=%s")
        params.append(int(total_api))
    if "missing_total" in cols:
        sets.append("missing_total=%s")
        params.append(int(missing_total))
    if "total_local" in cols:
        sets.append("total_local=%s")
        params.append(int(total_local))

    if not sets:
        return

    params.append(int(run_id))
    cur.execute(
        f"""
        update {SCHEMA}.audit_recent_run
           set {", ".join(sets)}
         where id=%s
        """,
        params,
    )


def get_control(cur):
    cols = get_table_cols(cur, SCHEMA, "range_scan_control")
    need = {"id_inicial", "id_final", "id_atual"}
    if not need.issubset(set(cols.keys())):
        raise RuntimeError("range_scan_control não tem id_inicial/id_final/id_atual (rode o kickoff).")

    row = pg_one(cur, f"select id_inicial, id_final, id_atual from {CONTROL_TABLE} limit 1")
    if not row:
        raise RuntimeError("range_scan_control vazio (rode o kickoff).")
    return row[0], row[1], row[2]


def set_id_atual(cur, v):
    cur.execute(f"update {CONTROL_TABLE} set id_atual=%s", (v,))


def http_get_json(url, params):
    full = f"{url}?{urlencode(params)}"
    last = None
    for i in range(1, ATTEMPTS + 1):
        try:
            req = urllib.request.Request(full, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=CONNECT_TIMEOUT + READ_TIMEOUT) as resp:
                data = resp.read()
                return resp.getcode(), json.loads(data.decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as e:
            code = e.code
            body = e.read()
            try:
                payload = json.loads(body.decode("utf-8", errors="replace")) if body else None
            except Exception:
                payload = None
            if code in (429, 500, 502, 503, 504):
                time.sleep(min(10, 2 ** (i - 1)))
                last = (code, payload)
                continue
            return code, payload
        except Exception as e:
            last = e
            time.sleep(min(10, 2 ** (i - 1)))
    raise last if last else RuntimeError("HTTP failure")


def api_list_ids(endpoint, low_id, high_id):
    flt = (
        f"id ge {int(low_id)} and id le {int(high_id)} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )
    ids = set()
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "includeDeletedItems": "true",
            "$filter": flt,
            "$select": SELECT_LIST,
            "$top": str(TOP),
            "$skip": str(skip),
            "$orderby": "id",
        }
        code, payload = http_get_json(f"{API_BASE}/{endpoint}", params)
        if code != 200 or not isinstance(payload, list) or not payload:
            break
        for x in payload:
            if isinstance(x, dict) and x.get("id") is not None:
                ids.add(int(x["id"]))
        if len(payload) < TOP:
            break
        skip += TOP
        if THROTTLE > 0:
            time.sleep(THROTTLE)
    return ids


def ids_in_detail(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.tickets_resolvidos_detail
        where ticket_id = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_abertos(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id::bigint
        from visualizacao_atual.tickets_abertos
        where ticket_id::bigint = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_mesclados(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.tickets_mesclados
        where ticket_id = any(%s) or merged_into_id = any(%s)
        """,
        (list(ids), list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_missing(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.audit_recent_missing
        where table_name = %s and ticket_id = any(%s)
        """,
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        f"""
        insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        [(int(run_id), TABLE_NAME, int(t)) for t in ids],
    )
    return len(ids)


def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        ensure_audit_tables(cur)

        id_inicial, id_final, id_atual = get_control(cur)

        if id_inicial is None or id_final is None:
            raise RuntimeError("range_scan_control id_inicial/id_final nulos (rode o kickoff).")

        if id_atual is None:
            print("done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null")
            return

        id_inicial = int(id_inicial)
        id_final = int(id_final)
        cursor = int(id_atual)

        if id_inicial < id_final:
            raise RuntimeError("range_scan_control está crescente (id_inicial < id_final). Esperado decrescente.")

        if cursor > id_inicial:
            cursor = id_inicial
        if cursor < id_final:
            set_id_atual(cur, None)
            conn.commit()
            print("done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null")
            return

        run_id = insert_run(cur, f"id-scan-api: usando range_scan_control id_inicial={id_inicial} id_final={id_final} id_atual={cursor}")
        conn.commit()

        t0 = time.time()
        total_ranges = 0
        total_api_ids = 0
        total_insert = 0
        total_excluded = 0

        for _ in range(max(1, ITERATIONS)):
            if (time.time() - t0) >= MAX_RUNTIME_SEC:
                break
            if cursor < id_final:
                break

            high_id = cursor
            low_id = max(id_final, cursor - BATCH_SIZE + 1)

            api_ids = set()
            api_ids |= api_list_ids("tickets", low_id, high_id)
            api_ids |= api_list_ids("tickets/past", low_id, high_id)

            total_ranges += 1
            total_api_ids += len(api_ids)

            in_detail = ids_in_detail(cur, api_ids)
            in_abertos = ids_in_abertos(cur, api_ids)
            in_mesclados = ids_in_mesclados(cur, api_ids)
            in_missing = ids_in_missing(cur, api_ids)

            excluded = in_detail | in_abertos | in_mesclados | in_missing
            total_excluded += len(excluded)

            candidates = sorted(list(api_ids - excluded))
            total_insert += insert_missing(cur, run_id, candidates)

            next_cursor = low_id - 1
            if next_cursor < id_final:
                cursor = None
                set_id_atual(cur, None)
                conn.commit()
                break

            cursor = int(next_cursor)
            set_id_atual(cur, cursor)
            conn.commit()

            if THROTTLE > 0:
                time.sleep(THROTTLE)

        update_run(cur, run_id, total_api_ids, total_insert, total_ranges)
        conn.commit()

        done = 1 if cursor is None else 0
        cnow = "null" if cursor is None else str(cursor)
        print(
            f"done={done} ranges={total_ranges} api_ids={total_api_ids} excluded={total_excluded} inserted_missing={total_insert} cursor_now={cnow} id_inicial={id_inicial} id_final={id_final}"
        )


if __name__ == "__main__":
    main()
