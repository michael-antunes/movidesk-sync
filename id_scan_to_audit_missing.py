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
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "10"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "60"))

START_ID_ENV = os.getenv("ID_SCAN_START_ID")
END_ID = int(os.getenv("ID_SCAN_END_ID", "1"))

BASE_STATUSES = ("Resolved", "Closed", "Canceled")
SELECT_LIST = "id,baseStatus"

CURSOR_SCHEMA = "visualizacao_resolvidos"
CURSOR_TABLE = "id_scan_api_cursor"


if not DSN:
    raise RuntimeError("NEON_DSN não definido")
if not TOKEN:
    raise RuntimeError("MOVIDESK_TOKEN não definido")


def pg_one(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchone()


def pg_all(cur, sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()


def get_columns(cur, schema: str, table: str):
    rows = pg_all(
        cur,
        """
        select column_name, is_nullable, column_default
        from information_schema.columns
        where table_schema=%s and table_name=%s
        """,
        (schema, table),
    )
    cols = {}
    for name, is_nullable, col_default in rows:
        cols[name] = (is_nullable == "NO", col_default)
    return cols


def ensure_audit_tables(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_start timestamptz")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_end timestamptz")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists total_api int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists missing_total int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists total_local int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists notes text")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists run_at timestamptz")

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists run_id bigint")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists table_name text")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists ticket_id integer")

    cur.execute(
        """
        create unique index if not exists audit_recent_missing_uniq
        on visualizacao_resolvidos.audit_recent_missing(table_name, ticket_id)
        """
    )


def ensure_cursor(cur, start_id: int):
    cur.execute(f"create schema if not exists {CURSOR_SCHEMA}")
    cur.execute(
        f"""
        create table if not exists {CURSOR_SCHEMA}.{CURSOR_TABLE}(
          cursor_id bigint not null
        )
        """
    )
    cur.execute(f"select count(*) from {CURSOR_SCHEMA}.{CURSOR_TABLE}")
    if int(cur.fetchone()[0] or 0) == 0:
        cur.execute(f"insert into {CURSOR_SCHEMA}.{CURSOR_TABLE}(cursor_id) values (%s)", (int(start_id),))


def get_cursor(cur) -> int:
    cur.execute(f"select cursor_id from {CURSOR_SCHEMA}.{CURSOR_TABLE} limit 1")
    return int(cur.fetchone()[0] or 0)


def set_cursor(cur, v: int):
    cur.execute(f"update {CURSOR_SCHEMA}.{CURSOR_TABLE} set cursor_id=%s", (int(v),))


def compute_default_start_id(cur) -> int:
    parts = []
    try:
        parts.append(int(pg_one(cur, "select coalesce(max(ticket_id),0) from visualizacao_resolvidos.tickets_resolvidos_detail")[0] or 0))
    except Exception:
        parts.append(0)

    try:
        parts.append(int(pg_one(cur, "select coalesce(max(ticket_id::bigint),0) from visualizacao_atual.tickets_abertos")[0] or 0))
    except Exception:
        parts.append(0)

    try:
        parts.append(int(pg_one(cur, "select coalesce(max(greatest(ticket_id, merged_into_id)),0) from visualizacao_resolvidos.tickets_mesclados")[0] or 0))
    except Exception:
        parts.append(0)

    mx = max(parts) if parts else 0
    return int(mx) if mx > 0 else 1


def create_run(cur, notes: str) -> int:
    cols = get_columns(cur, "visualizacao_resolvidos", "audit_recent_run")

    required = [c for c, (notnull, default) in cols.items() if notnull and default is None and c != "id"]
    supported = {
        "window_start",
        "window_end",
        "total_api",
        "missing_total",
        "total_local",
        "notes",
        "run_at",
    }
    missing_required = [c for c in required if c not in supported]
    if missing_required:
        raise RuntimeError(f"audit_recent_run tem colunas obrigatórias sem default não suportadas: {missing_required}")

    fields = []
    values_sql = []
    params = []

    if "window_start" in cols:
        fields.append("window_start")
        values_sql.append("now()")
    if "window_end" in cols:
        fields.append("window_end")
        values_sql.append("now()")
    if "total_api" in cols:
        fields.append("total_api")
        values_sql.append("%s")
        params.append(0)
    if "missing_total" in cols:
        fields.append("missing_total")
        values_sql.append("%s")
        params.append(0)
    if "total_local" in cols:
        fields.append("total_local")
        values_sql.append("%s")
        params.append(0)
    if "run_at" in cols:
        fields.append("run_at")
        values_sql.append("now()")
    if "notes" in cols:
        fields.append("notes")
        values_sql.append("%s")
        params.append(notes)

    if not fields:
        cur.execute("insert into visualizacao_resolvidos.audit_recent_run default values returning id")
        return int(cur.fetchone()[0])

    cur.execute(
        f"""
        insert into visualizacao_resolvidos.audit_recent_run({",".join(fields)})
        values ({",".join(values_sql)})
        returning id
        """,
        params,
    )
    return int(cur.fetchone()[0])


def update_run(cur, run_id: int, total_api: int, missing_total: int, total_local: int):
    cols = get_columns(cur, "visualizacao_resolvidos", "audit_recent_run")
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
        update visualizacao_resolvidos.audit_recent_run
           set {", ".join(sets)}
         where id=%s
        """,
        params,
    )


def http_get_json(url, params):
    q = urlencode(params)
    full = f"{url}?{q}"
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


def api_list_ids(endpoint, low_id: int, high_id: int):
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
        if code != 200 or not isinstance(payload, list):
            break
        if not payload:
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
        """
        select ticket_id
        from visualizacao_resolvidos.tickets_resolvidos_detail
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
        """
        select ticket_id
        from visualizacao_resolvidos.tickets_mesclados
        where ticket_id = any(%s) or merged_into_id = any(%s)
        """,
        (list(ids), list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_missing(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = %s and ticket_id = any(%s)
        """,
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def insert_missing(cur, run_id: int, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        """
        insert into visualizacao_resolvidos.audit_recent_missing(run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        [(int(run_id), TABLE_NAME, int(t)) for t in ids],
    )
    return len(ids)


def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        ensure_audit_tables(cur)

        if START_ID_ENV:
            start_id = int(START_ID_ENV)
        else:
            start_id = compute_default_start_id(cur)

        ensure_cursor(cur, start_id)
        cursor = get_cursor(cur)
        if cursor <= 0:
            cursor = start_id
            set_cursor(cur, cursor)

        run_id = create_run(cur, f"id-scan-api: range {cursor}->{END_ID} | insere faltantes (detail/abertos/mesclados)")
        conn.commit()

        total_ranges = 0
        total_api_ids = 0
        total_insert = 0
        total_excluded = 0

        for _ in range(max(1, ITERATIONS)):
            if cursor < END_ID:
                break

            high_id = int(cursor)
            low_id = int(max(END_ID, cursor - BATCH_SIZE + 1))

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

            cursor = low_id - 1
            set_cursor(cur, cursor)
            conn.commit()

            if THROTTLE > 0:
                time.sleep(THROTTLE)

        update_run(cur, run_id, total_api_ids, total_insert, total_ranges)
        conn.commit()

        print(
            f"ranges={total_ranges} api_ids={total_api_ids} excluded={total_excluded} inserted_missing={total_insert} cursor_now={cursor}"
        )


if __name__ == "__main__":
    main()
