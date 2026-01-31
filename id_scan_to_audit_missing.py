import os
import time
import datetime as dt
import psycopg2
from psycopg2.extras import execute_values
import requests

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "20"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

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
        f"create unique index if not exists audit_recent_missing_uniq on {SCHEMA}.audit_recent_missing(table_name, ticket_id)"
    )

def create_run(cur):
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
    add_expr("api_ids", "0")
    add_expr("inserted_missing", "0")
    add_expr("range_count", "0")
    add_param("table_name", TABLE_NAME)
    add_param("notes", "id-scan")

    cur.execute(
        f"insert into {SCHEMA}.audit_recent_run({','.join(ins_cols)}) values ({','.join(ins_vals)}) returning id",
        tuple(params),
    )
    return int(cur.fetchone()[0])

def update_run(cur, run_id, total_api, inserted_missing, range_count):
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
        params.append(int(inserted_missing))
    if "api_ids" in cols:
        sets.append("api_ids=%s")
        params.append(int(total_api))
    if "inserted_missing" in cols:
        sets.append("inserted_missing=%s")
        params.append(int(inserted_missing))
    if "range_count" in cols:
        sets.append("range_count=%s")
        params.append(int(range_count))

    if not sets:
        return

    params.append(int(run_id))
    cur.execute(
        f"update {SCHEMA}.audit_recent_run set {', '.join(sets)} where id=%s",
        tuple(params),
    )

def get_control(cur):
    cols = table_cols(cur, SCHEMA, "range_scan_control")
    if "id_inicial" not in cols or "id_final" not in cols:
        return None, None, None
    cur.execute(
        f"select id_inicial, id_final, id_atual from {SCHEMA}.range_scan_control order by id desc limit 1"
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]

def set_id_atual(cur, id_atual):
    cols = table_cols(cur, SCHEMA, "range_scan_control")
    if "id_atual" not in cols:
        return
    cur.execute(
        f"update {SCHEMA}.range_scan_control set id_atual=%s where id=(select id from {SCHEMA}.range_scan_control order by id desc limit 1)",
        (id_atual,),
    )

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

def api_list_id_lastupdate(endpoint, low_id, high_id):
    url = f"{API_BASE}/{endpoint}"
    params = {
        "token": TOKEN,
        "$select": "id,lastUpdate",
        "$filter": f"id ge {int(low_id)} and id le {int(high_id)} and (baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')",
        "$orderby": "id",
        "$top": int(TOP),
        "includeDeletedItems": "true",
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:800]}")
    data = r.json() or []
    out = {}
    for x in data:
        try:
            tid = int(x.get("id"))
        except Exception:
            continue
        lu = parse_api_dt(x.get("lastUpdate"))
        if lu is None:
            continue
        prev = out.get(tid)
        if prev is None or lu > prev:
            out[tid] = lu
    return out

def detail_last_update_map(cur, ids):
    if not ids:
        return {}
    cur.execute(
        f"select ticket_id, last_update from {SCHEMA}.tickets_resolvidos_detail where ticket_id = any(%s)",
        (list(ids),),
    )
    return {int(r[0]): r[1] for r in cur.fetchall()}

def upsert_missing(cur, run_id, ids):
    if not ids:
        return 0
    cols = table_cols(cur, SCHEMA, "audit_recent_missing")
    ins_cols = ["run_id", "table_name", "ticket_id"]
    template_parts = ["%s", "%s", "%s"]
    upd_sets = ["run_id=excluded.run_id"]

    if "first_seen" in cols:
        ins_cols.append("first_seen")
        template_parts.append("now()")
    if "last_seen" in cols:
        ins_cols.append("last_seen")
        template_parts.append("now()")
        upd_sets.append("last_seen=now()")
    if "attempts" in cols:
        ins_cols.append("attempts")
        template_parts.append("0")

    template = "(" + ",".join(template_parts) + ")"

    sql = (
        f"insert into {SCHEMA}.audit_recent_missing({','.join(ins_cols)}) values %s "
        f"on conflict (table_name, ticket_id) do update set {', '.join(upd_sets)}"
    )

    execute_values(
        cur,
        sql,
        [(int(run_id), TABLE_NAME, int(tid)) for tid in ids],
        template=template,
        page_size=1000,
    )
    return len(ids)

def main():
    with conn() as c, c.cursor() as cur:
        ensure_missing_unique(cur)
        id_inicial, id_final, id_atual = get_control(cur)
        if id_inicial is None or id_final is None:
            print("done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null")
            return

        cursor = int(id_atual) if id_atual is not None else int(id_inicial)
        if cursor < int(id_final):
            set_id_atual(cur, None)
            c.commit()
            print(f"done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null id_inicial={id_inicial} id_final={id_final}")
            return

        run_id = create_run(cur)
        c.commit()

        total_ranges = 0
        total_api_ids = 0
        total_excluded = 0
        total_insert = 0

        for _ in range(int(ITERATIONS)):
            high_id = cursor
            low_id = max(int(id_final), cursor - int(BATCH_SIZE) + 1)

            api_map = {}
            for endpoint in ("tickets", "tickets/past"):
                m = api_list_id_lastupdate(endpoint, low_id, high_id)
                for tid, lu in m.items():
                    prev = api_map.get(tid)
                    if prev is None or lu > prev:
                        api_map[tid] = lu

            api_ids = set(api_map.keys())

            total_ranges += 1
            total_api_ids += len(api_ids)

            detail_map = detail_last_update_map(cur, api_ids)

            candidates = []
            for tid, api_dt in api_map.items():
                api_dt = normalize_dt(api_dt)
                db_dt = normalize_dt(detail_map.get(tid))
                if api_dt is None:
                    continue
                if db_dt is None or api_dt > db_dt:
                    candidates.append(int(tid))

            candidates = sorted(candidates)
            excluded = len(api_ids) - len(candidates)
            total_excluded += excluded

            total_insert += upsert_missing(cur, run_id, candidates)
            c.commit()

            next_cursor = low_id - 1
            if next_cursor < int(id_final):
                cursor = None
                set_id_atual(cur, None)
                c.commit()
                break

            cursor = int(next_cursor)
            set_id_atual(cur, cursor)
            c.commit()

            if THROTTLE > 0:
                time.sleep(float(THROTTLE))

        update_run(cur, run_id, total_api_ids, total_insert, total_ranges)
        c.commit()

        done = 1 if cursor is None else 0
        cnow = "null" if cursor is None else str(cursor)
        print(
            f"done={done} ranges={total_ranges} api_ids={total_api_ids} excluded={total_excluded} inserted_missing={total_insert} cursor_now={cnow} id_inicial={id_inicial} id_final={id_final}"
        )

if __name__ == "__main__":
    main()
