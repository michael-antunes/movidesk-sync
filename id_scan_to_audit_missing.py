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

BATCH_SIZE = int(os.getenv("ID_VALIDATE_BATCH_SIZE", "50"))
MAX_RUNTIME_SEC = int(os.getenv("ID_VALIDATE_MAX_RUNTIME_SEC", "240"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "60"))

SELECT_MIN = "id,baseStatus"
BASE_STATUSES = {"Resolved", "Closed", "Canceled"}

CURSOR_SCHEMA = os.getenv("ID_VALIDATE_CURSOR_SCHEMA", "visualizacao_resolvidos")
CURSOR_TABLE = os.getenv("ID_VALIDATE_CURSOR_TABLE", "id_validate_cursor_resolvidos")


if not DSN:
    raise RuntimeError("NEON_DSN não definido")
if not TOKEN:
    raise RuntimeError("MOVIDESK_TOKEN não definido")


def get_columns(cur, schema: str, table: str):
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema=%s and table_name=%s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def ensure_cursor(cur):
    cur.execute(f"create schema if not exists {CURSOR_SCHEMA}")
    cur.execute(
        f"""
        create table if not exists {CURSOR_SCHEMA}.{CURSOR_TABLE}(
          cursor_id bigint not null
        )
        """
    )
    cur.execute(f"select count(*) from {CURSOR_SCHEMA}.{CURSOR_TABLE}")
    if cur.fetchone()[0] == 0:
        cur.execute("select coalesce(max(ticket_id),0) from visualizacao_resolvidos.tickets_resolvidos")
        mx = int(cur.fetchone()[0] or 0)
        cur.execute(f"insert into {CURSOR_SCHEMA}.{CURSOR_TABLE}(cursor_id) values (%s)", (mx,))


def get_cursor(cur) -> int:
    cur.execute(f"select cursor_id from {CURSOR_SCHEMA}.{CURSOR_TABLE} limit 1")
    return int(cur.fetchone()[0] or 0)


def set_cursor(cur, v: int):
    cur.execute(f"update {CURSOR_SCHEMA}.{CURSOR_TABLE} set cursor_id=%s", (int(v),))


def ensure_missing_tables(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          window_start timestamptz,
          window_end timestamptz,
          total_api int,
          missing_total int,
          total_local int,
          notes text
        )
        """
    )
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )
    cur.execute(
        """
        create unique index if not exists audit_recent_missing_uniq
        on visualizacao_resolvidos.audit_recent_missing(table_name, ticket_id)
        """
    )


def create_run(cur, notes: str) -> int:
    cols = get_columns(cur, "visualizacao_resolvidos", "audit_recent_run")
    fields = []
    values = []
    params = []

    def add(col, val):
        fields.append(col)
        values.append("%s")
        params.append(val)

    if "window_start" in cols:
        add("window_start", "now()")
    if "window_end" in cols:
        add("window_end", "now()")
    if "total_api" in cols:
        add("total_api", 0)
    if "missing_total" in cols:
        add("missing_total", 0)
    if "total_local" in cols:
        add("total_local", 0)
    if "notes" in cols:
        add("notes", notes)

    sql_fields = []
    sql_values = []
    sql_params = []
    for i, f in enumerate(fields):
        sql_fields.append(f)
        if params[i] == "now()":
            sql_values.append("now()")
        else:
            sql_values.append(values[i])
            sql_params.append(params[i])

    if not sql_fields:
        cur.execute("insert into visualizacao_resolvidos.audit_recent_run default values returning id")
        return int(cur.fetchone()[0])

    cur.execute(
        f"""
        insert into visualizacao_resolvidos.audit_recent_run({",".join(sql_fields)})
        values ({",".join(sql_values)})
        returning id
        """,
        sql_params,
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


def api_exists_and_status(ticket_id: int):
    p1 = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$select": SELECT_MIN,
        "id": str(int(ticket_id)),
    }
    c1, j1 = http_get_json(f"{API_BASE}/tickets", p1)
    if c1 == 200 and isinstance(j1, dict) and j1.get("id") is not None:
        return True, str(j1.get("baseStatus") or "")

    p2 = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$select": SELECT_MIN,
        "$filter": f"id eq {int(ticket_id)}",
        "$top": "1",
    }
    c2, j2 = http_get_json(f"{API_BASE}/tickets/past", p2)
    if c2 == 200 and isinstance(j2, list) and len(j2) > 0 and isinstance(j2[0], dict) and j2[0].get("id") is not None:
        return True, str(j2[0].get("baseStatus") or "")

    return False, ""


def fetch_candidates(cur, cursor_id: int, limit: int):
    cur.execute(
        """
        select tr.ticket_id
          from visualizacao_resolvidos.tickets_resolvidos tr
         where tr.ticket_id <= %s
           and not exists (select 1 from visualizacao_resolvidos.tickets_resolvidos_detail d where d.ticket_id = tr.ticket_id)
           and not exists (select 1 from visualizacao_atual.tickets_abertos a where a.ticket_id::bigint = tr.ticket_id)
           and not exists (
             select 1
               from visualizacao_resolvidos.tickets_mesclados m
              where m.ticket_id = tr.ticket_id
                 or m.merged_into_id = tr.ticket_id
           )
           and not exists (
             select 1
               from visualizacao_resolvidos.audit_recent_missing miss
              where miss.table_name = %s
                and miss.ticket_id = tr.ticket_id
           )
         order by tr.ticket_id desc
         limit %s
        """,
        (int(cursor_id), TABLE_NAME, int(limit)),
    )
    return [int(r[0]) for r in cur.fetchall()]


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
        [(int(run_id), TABLE_NAME, int(i)) for i in ids],
    )
    return len(ids)


def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        ensure_missing_tables(cur)
        ensure_cursor(cur)
        run_id = create_run(cur, "id-validate-api: candidatos de tickets_resolvidos; valida /tickets + /tickets/past; insere audit_recent_missing")
        conn.commit()

        t0 = time.time()
        cursor_id = get_cursor(cur)

        total_local = 0
        total_api = 0
        total_inserted = 0

        while cursor_id > 0 and (time.time() - t0) < MAX_RUNTIME_SEC:
            batch = fetch_candidates(cur, cursor_id, BATCH_SIZE)
            if not batch:
                break

            total_local += len(batch)

            valid = []
            for tid in batch:
                ok, bs = api_exists_and_status(tid)
                total_api += 1
                if ok and bs in BASE_STATUSES:
                    valid.append(tid)
                if THROTTLE > 0:
                    time.sleep(THROTTLE)

            total_inserted += insert_missing(cur, run_id, valid)

            cursor_id = min(batch) - 1
            set_cursor(cur, cursor_id)
            conn.commit()

        update_run(cur, run_id, total_api, total_inserted, total_local)
        conn.commit()

        print(f"scanned_local={total_local} api_checked={total_api} inserted_missing={total_inserted} cursor_now={cursor_id}")


if __name__ == "__main__":
    main()
