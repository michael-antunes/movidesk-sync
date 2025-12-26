import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import execute_values
import requests

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

PAGE_TOP = int(os.getenv("RANGE_SCAN_PAGE_TOP", "100"))
LIMIT = int(os.getenv("RANGE_SCAN_LIMIT", "400"))
THROTTLE = float(os.getenv("RANGE_SCAN_THROTTLE", "0.25"))
MAX_RUNTIME_SEC = int(os.getenv("RANGE_SCAN_MAX_RUNTIME_SEC", "240"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

def conn():
    return psycopg2.connect(DSN)

def parse_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def to_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def iso_z(dt):
    dt = to_utc(dt)
    return dt.isoformat().replace("+00:00", "Z")

def fetch_page(data_fim, ultima, skip):
    params = {
        "token": TOKEN,
        "$select": "id,lastUpdate,baseStatus",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {iso_z(data_fim)} and lastUpdate le {iso_z(ultima)}",
        "$orderby": "lastUpdate desc",
        "$top": int(PAGE_TOP),
        "$skip": int(skip),
        "includeDeletedItems": "true",
    }
    r = requests.get(API, params=params, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:800]}")
    return r.json() or []

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

def create_run(cur, data_fim, ultima):
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
    add_expr("total_api", "0")
    add_expr("missing_total", "0")
    add_expr("run_at", "now()")
    add_param("window_from", data_fim)
    add_param("window_to", ultima)
    add_param("notes", "range-scan")

    cur.execute(
        f"insert into {SCHEMA}.audit_recent_run({','.join(ins_cols)}) values ({','.join(ins_vals)}) returning id",
        tuple(params),
    )
    return int(cur.fetchone()[0])

def update_run(cur, run_id, total_api, missing_total):
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
        params.append(int(missing_total))

    if not sets:
        return

    params.append(int(run_id))
    cur.execute(
        f"update {SCHEMA}.audit_recent_run set {', '.join(sets)} where id=%s",
        tuple(params),
    )

def existing_in_missing(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.audit_recent_missing
        where table_name=%s and ticket_id = any(%s)
        """,
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}

def existing_in_abertos(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_atual.tickets_abertos
        where ticket_id = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}

def existing_in_detail(cur, ids):
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

def mesclados_sets(cur, ids):
    if not ids:
        return set()
    cols = table_cols(cur, SCHEMA, "tickets_mesclados")
    if not cols:
        return set()
    cur.execute(
        f"""
        select ticket_id as id
        from {SCHEMA}.tickets_mesclados
        where ticket_id = any(%s)
        union
        select merged_into_id as id
        from {SCHEMA}.tickets_mesclados
        where merged_into_id = any(%s)
        """,
        (list(ids), list(ids)),
    )
    out = set()
    for (v,) in cur.fetchall():
        if v is not None:
            out.add(int(v))
    return out

def read_control(cur):
    cur.execute(
        f"""
        select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio)
        from {SCHEMA}.range_scan_control
        limit 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    data_inicio, data_fim, ultima = row[0], row[1], row[2]
    data_inicio = to_utc(parse_dt(data_inicio))
    data_fim = to_utc(parse_dt(data_fim))
    ultima = to_utc(parse_dt(ultima))
    if data_inicio is None or data_fim is None or ultima is None:
        raise RuntimeError("range_scan_control com datas inválidas")
    if ultima > data_inicio:
        ultima = data_inicio
    if ultima < data_fim:
        ultima = data_fim
    return data_inicio, data_fim, ultima

def set_ultima_validada(cur, nv):
    cur.execute(
        f"update {SCHEMA}.range_scan_control set ultima_data_validada=%s",
        (nv,),
    )

def hit_end(cur):
    cur.execute(
        f"""
        select (coalesce(ultima_data_validada, data_inicio) <= data_fim)
        from {SCHEMA}.range_scan_control
        limit 1
        """
    )
    return bool(cur.fetchone()[0])

def do_one_cycle(run_id):
    ids = []
    api_last = {}
    min_lu = None
    skip = 0

    with conn() as c, c.cursor() as cur:
        ctrl = read_control(cur)
        if ctrl is None:
            return True, 0, 0

        _, data_fim, ultima = ctrl

        while True:
            page = fetch_page(data_fim, ultima, skip)
            if not page:
                break

            for t in page:
                lu = to_utc(parse_dt(t.get("lastUpdate")))
                if lu is None:
                    continue
                if lu < data_fim or lu > ultima:
                    continue
                try:
                    tid = int(t.get("id"))
                except Exception:
                    continue
                if tid not in api_last:
                    ids.append(tid)
                    api_last[tid] = lu
                    if min_lu is None or lu < min_lu:
                        min_lu = lu
                if len(ids) >= LIMIT:
                    break

            if len(ids) >= LIMIT:
                break
            if len(page) < PAGE_TOP:
                break

            skip += PAGE_TOP
            if THROTTLE > 0:
                time.sleep(THROTTLE)

        inserted = 0
        if ids:
            in_missing = existing_in_missing(cur, ids)
            in_abertos = existing_in_abertos(cur, ids)
            in_detail = existing_in_detail(cur, ids)
            in_mesclados = mesclados_sets(cur, ids)

            to_insert = []
            for tid in ids:
                if tid in in_missing:
                    continue
                if tid in in_abertos:
                    continue
                if tid in in_detail:
                    continue
                if tid in in_mesclados:
                    continue
                to_insert.append(tid)

            if to_insert:
                execute_values(
                    cur,
                    f"insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id) values %s",
                    [(int(run_id), TABLE_NAME, int(tid)) for tid in to_insert],
                    template="(%s,%s,%s)",
                    page_size=1000,
                )
                inserted = len(to_insert)

        nv = min_lu
        if nv is None:
            nv = data_fim
        else:
            nv = nv - timedelta(microseconds=1)
            if nv < data_fim:
                nv = data_fim

        set_ultima_validada(cur, nv)

        c.commit()
        return hit_end(cur), len(ids), inserted

def main():
    start = time.time()
    total_api = 0
    total_missing = 0

    with conn() as c, c.cursor() as cur:
        ctrl = read_control(cur)
        if ctrl is None:
            print("[range-scan] sem range_scan_control")
            return
        _, data_fim, ultima = ctrl
        run_id = create_run(cur, data_fim, ultima)
        c.commit()

    while True:
        hit, got, ins = do_one_cycle(run_id)
        total_api += int(got)
        total_missing += int(ins)
        print(f"[range-scan] ciclo: tickets_api={got} inserted_missing={ins} hit_end={hit}")

        if hit:
            break
        if time.time() - start >= MAX_RUNTIME_SEC:
            print(f"[range-scan] tempo esgotado ({time.time()-start:.1f}s >= {MAX_RUNTIME_SEC}s). Encerrando este job.")
            break

    with conn() as c, c.cursor() as cur:
        update_run(cur, run_id, total_api, total_missing)
        c.commit()

if __name__ == "__main__":
    main()
