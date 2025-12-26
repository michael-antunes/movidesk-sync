import os
import time
from datetime import datetime, timezone

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

def ensure_run_id(cur):
    cols = table_cols(cur, SCHEMA, "audit_recent_run")
    if not cols:
        cur.execute(f"insert into {SCHEMA}.audit_recent_run default values returning id")
        return int(cur.fetchone()[0])

    if "table_name" in cols:
        cur.execute(
            f"insert into {SCHEMA}.audit_recent_run(table_name) values (%s) returning id",
            (TABLE_NAME,),
        )
        return int(cur.fetchone()[0])

    cur.execute(f"insert into {SCHEMA}.audit_recent_run default values returning id")
    return int(cur.fetchone()[0])

def update_run(cur, run_id, api_ids, inserted_missing, cycles):
    cols = table_cols(cur, SCHEMA, "audit_recent_run")
    sets = []
    args = []
    if "api_ids" in cols:
        sets.append("api_ids=%s")
        args.append(int(api_ids))
    if "inserted_missing" in cols:
        sets.append("inserted_missing=%s")
        args.append(int(inserted_missing))
    if "range_count" in cols:
        sets.append("range_count=%s")
        args.append(int(cycles))
    if "updated_at" in cols:
        sets.append("updated_at=now()")
    if not sets:
        return
    args.append(int(run_id))
    cur.execute(
        f"update {SCHEMA}.audit_recent_run set {', '.join(sets)} where id=%s",
        tuple(args),
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

def mesclados_info(cur, ids):
    if not ids:
        return {}, set(), set()

    cols = table_cols(cur, SCHEMA, "tickets_mesclados")
    if not cols:
        return {}, set(), set()

    lu_col = None
    for c in ("last_update", "updated_at", "lastupdate", "lastUpdate", "last_updated_at"):
        if c in cols:
            lu_col = c
            break
    if lu_col is None:
        for c in cols:
            lc = c.lower()
            if "last" in lc and "update" in lc:
                lu_col = c
                break

    ticket_last = {}
    ticket_ids = set()
    if lu_col:
        cur.execute(
            f"""
            select ticket_id, {lu_col}
            from {SCHEMA}.tickets_mesclados
            where ticket_id = any(%s)
            """,
            (list(ids),),
        )
        for tid, lu in cur.fetchall():
            if tid is None or lu is None:
                continue
            ticket_ids.add(int(tid))
            ticket_last[int(tid)] = to_utc(parse_dt(lu))
    else:
        cur.execute(
            f"""
            select ticket_id
            from {SCHEMA}.tickets_mesclados
            where ticket_id = any(%s)
            """,
            (list(ids),),
        )
        for (tid,) in cur.fetchall():
            if tid is not None:
                ticket_ids.add(int(tid))

    cur.execute(
        f"""
        select merged_into_id
        from {SCHEMA}.tickets_mesclados
        where merged_into_id = any(%s)
        """,
        (list(ids),),
    )
    merged_into = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}

    return ticket_last, ticket_ids, merged_into

def read_control():
    with conn() as c, c.cursor() as cur:
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
        if ultima is None or data_inicio is None or data_fim is None:
            raise RuntimeError("range_scan_control com datas inválidas")
        if ultima > data_inicio:
            ultima = data_inicio
        if ultima < data_fim:
            ultima = data_fim
        return data_inicio, data_fim, ultima

def set_ultima_validada(nv):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"update {SCHEMA}.range_scan_control set ultima_data_validada=%s",
            (nv,),
        )
        c.commit()

def hit_end_now():
    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"""
            select (coalesce(ultima_data_validada, data_inicio) <= data_fim)
            from {SCHEMA}.range_scan_control
            limit 1
            """
        )
        return bool(cur.fetchone()[0])

def do_one_cycle(run_id):
    ctrl = read_control()
    if ctrl is None:
        return True, 0, 0, None

    data_inicio, data_fim, ultima = ctrl

    ids = []
    api_last = {}
    min_lu = None
    skip = 0

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

    with conn() as c, c.cursor() as cur:
        if ids:
            in_missing = existing_in_missing(cur, ids)
            in_abertos = existing_in_abertos(cur, ids)
            in_detail = existing_in_detail(cur, ids)
            mesclado_last, mesclado_ticket_ids, merged_into = mesclados_info(cur, ids)

            to_insert = []
            for tid in ids:
                if tid in in_missing:
                    continue
                if tid in in_abertos:
                    continue
                if tid in in_detail:
                    continue
                if tid in merged_into:
                    continue

                if tid in mesclado_ticket_ids:
                    api_dt = api_last.get(tid)
                    m_dt = mesclado_last.get(tid)
                    if api_dt is None or m_dt is None:
                        continue
                    if api_dt <= m_dt:
                        continue

                to_insert.append(tid)

            if to_insert:
                execute_values(
                    cur,
                    f"""
                    insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id, first_seen, last_seen, attempts)
                    values %s
                    on conflict (table_name, ticket_id) do update
                      set last_seen = excluded.last_seen,
                          run_id = excluded.run_id
                    """,
                    [(int(run_id), TABLE_NAME, int(tid), "now()", "now()", 0) for tid in to_insert],
                    template="(%s,%s,%s,now(),now(),0)",
                    page_size=1000,
                )
                inserted = len(to_insert)
        c.commit()

    nv = min_lu if min_lu is not None else data_fim
    if nv < data_fim:
        nv = data_fim
    set_ultima_validada(nv)

    return hit_end_now(), len(ids), inserted, nv

def main():
    start = time.time()
    cycles = 0
    api_total = 0
    inserted_total = 0

    with conn() as c, c.cursor() as cur:
        run_id = ensure_run_id(cur)
        c.commit()

    while True:
        hit_end, got, inserted, nv = do_one_cycle(run_id)
        cycles += 1
        api_total += int(got)
        inserted_total += int(inserted)
        print(f"[range-scan] ciclo: tickets_api={got} inserted_missing={inserted} hit_end={hit_end}")
        if hit_end:
            break
        if time.time() - start >= MAX_RUNTIME_SEC:
            print(f"[range-scan] tempo esgotado ({time.time()-start:.1f}s >= {MAX_RUNTIME_SEC}s). Encerrando este job.")
            break

    with conn() as c, c.cursor() as cur:
        update_run(cur, run_id, api_total, inserted_total, cycles)
        c.commit()

if __name__ == "__main__":
    main()
