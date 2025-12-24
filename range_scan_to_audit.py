import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
import requests

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

PAGE_TOP = int(os.getenv("RANGE_SCAN_PAGE_TOP", "100"))
LIMIT = int(os.getenv("RANGE_SCAN_LIMIT", "400"))
THROTTLE = float(os.getenv("RANGE_SCAN_THROTTLE", "0.25"))
MAX_RUNTIME_SEC = int(os.getenv("RANGE_SCAN_MAX_RUNTIME_SEC", "240"))
SLEEP_SEC = float(os.getenv("RANGE_SCAN_SLEEP_SEC", "0"))

SCHEMA = "visualizacao_resolvidos"
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")


def conn():
    return psycopg2.connect(DSN)


def to_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = parse_dt(dt)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def iso_z(dt):
    dt = to_utc(dt)
    return dt.isoformat().replace("+00:00", "Z")


def parse_dt(s):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_page(params):
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    return data or []


def audit_recent_run_cols(cur):
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema=%s and table_name='audit_recent_run'
        """,
        (SCHEMA,),
    )
    return {r[0] for r in cur.fetchall()}


def ensure_run_id(cur, data_fim, ultima):
    cur.execute(f"select max(id) from {SCHEMA}.audit_recent_run")
    rid = cur.fetchone()[0]
    if rid is not None:
        return int(rid)

    cols = audit_recent_run_cols(cur)
    ins_cols = []
    ins_vals = []
    params = []

    if "started_at" in cols:
        ins_cols.append("started_at")
        ins_vals.append("now()")
    if "window_start" in cols:
        ins_cols.append("window_start")
        ins_vals.append("now()")
    if "window_end" in cols:
        ins_cols.append("window_end")
        ins_vals.append("now()")
    if "total_api" in cols:
        ins_cols.append("total_api")
        ins_vals.append("0")
    if "missing_total" in cols:
        ins_cols.append("missing_total")
        ins_vals.append("0")
    if "run_at" in cols:
        ins_cols.append("run_at")
        ins_vals.append("now()")
    if "window_from" in cols:
        ins_cols.append("window_from")
        ins_vals.append("%s")
        params.append(data_fim)
    if "window_to" in cols:
        ins_cols.append("window_to")
        ins_vals.append("%s")
        params.append(ultima)
    if "notes" in cols:
        ins_cols.append("notes")
        ins_vals.append("%s")
        params.append("range-scan")

    if not ins_cols:
        cur.execute(f"insert into {SCHEMA}.audit_recent_run default values returning id")
        return int(cur.fetchone()[0])

    cur.execute(
        f"insert into {SCHEMA}.audit_recent_run({','.join(ins_cols)}) values ({','.join(ins_vals)}) returning id",
        tuple(params),
    )
    return int(cur.fetchone()[0])


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


def detail_last_updates(cur, ids):
    if not ids:
        return {}
    cur.execute(
        f"""
        select ticket_id, last_update
        from {SCHEMA}.tickets_resolvidos_detail
        where ticket_id = any(%s)
        """,
        (list(ids),),
    )
    out = {}
    for tid, lu in cur.fetchall():
        if tid is None:
            continue
        out[int(tid)] = to_utc(lu)
    return out


def mesclados_info(cur, ids):
    if not ids:
        return {}, set()

    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema=%s and table_name='tickets_mesclados'
        """,
        (SCHEMA,),
    )
    cols = {r[0] for r in cur.fetchall()}
    if not cols:
        return {}, set()

    lu_col = None
    for c in ("last_update", "lastupdate", "updated_at", "last_updated_at"):
        if c in cols:
            lu_col = c
            break
    if lu_col is None:
        for c in cols:
            lc = c.lower()
            if "last" in lc and "update" in lc:
                lu_col = c
                break

    mesclado_ticket_last = {}
    if lu_col:
        try:
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
                mesclado_ticket_last[int(tid)] = to_utc(lu)
        except Exception:
            mesclado_ticket_last = {}

    cur.execute(
        f"""
        select merged_into_id
        from {SCHEMA}.tickets_mesclados
        where merged_into_id = any(%s)
        """,
        (list(ids),),
    )
    merged_into = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}

    return mesclado_ticket_last, merged_into


def do_one_cycle():
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
            return True, 0

        data_inicio, data_fim, ultima = row[0], row[1], row[2]
        data_inicio = to_utc(data_inicio)
        data_fim = to_utc(data_fim)
        ultima = to_utc(ultima)

    ids = []
    api_last = {}
    min_lu = None
    skip = 0

    while True:
        params = {
            "token": TOKEN,
            "$select": "id,baseStatus,lastUpdate",
            "$filter": "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') "
            f"and lastUpdate ge {iso_z(data_fim)} and lastUpdate le {iso_z(ultima)}",
            "$orderby": "lastUpdate desc",
            "$top": PAGE_TOP,
            "$skip": skip,
        }
        page = fetch_page(params)
        if not page:
            break

        for t in page:
            lu = to_utc(parse_dt(t.get("lastUpdate")))
            if not lu:
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

    missing = []

    with conn() as c, c.cursor() as cur:
        if ids:
            rid = ensure_run_id(cur, data_fim, ultima)

            in_missing = existing_in_missing(cur, ids)
            in_abertos = existing_in_abertos(cur, ids)
            detail_last = detail_last_updates(cur, ids)
            mesclado_ticket_last, merged_into = mesclados_info(cur, ids)

            for tid in ids:
                if tid in in_missing:
                    continue
                if tid in in_abertos:
                    continue
                if tid in merged_into:
                    continue

                api_dt = api_last.get(tid)

                m_dt = mesclado_ticket_last.get(tid)
                if m_dt is not None and api_dt is not None and api_dt <= m_dt:
                    continue

                db_dt = detail_last.get(tid)
                if db_dt is None:
                    missing.append(tid)
                else:
                    if api_dt is None:
                        continue
                    if db_dt != api_dt:
                        missing.append(tid)

            if missing:
                execute_values(
                    cur,
                    f"""
                    insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                    """,
                    [(rid, TABLE_NAME, int(m)) for m in missing],
                    page_size=1000,
                )

        nv = min_lu if min_lu is not None else data_fim
        cur.execute(
            f"update {SCHEMA}.range_scan_control set ultima_data_validada=%s",
            (nv,),
        )
        c.commit()

    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"""
            select (coalesce(ultima_data_validada, data_inicio) <= data_fim)
            from {SCHEMA}.range_scan_control
            limit 1
            """
        )
        hit_end = bool(cur.fetchone()[0])

    return hit_end, len(ids)


def main():
    start = time.time()
    while True:
        hit_end, got = do_one_cycle()
        print(f"[range-scan] ciclo: tickets_api={got} hit_end={hit_end}")
        if hit_end:
            print("[range-scan] FIM: ultima_data_validada já alcançou data_fim.")
            break
        elapsed = time.time() - start
        if elapsed >= MAX_RUNTIME_SEC:
            print(f"[range-scan] tempo esgotado ({elapsed:.1f}s >= {MAX_RUNTIME_SEC}s). Encerrando este job.")
            break
        if SLEEP_SEC > 0:
            time.sleep(min(SLEEP_SEC, max(0, MAX_RUNTIME_SEC - (time.time() - start))))


if __name__ == "__main__":
    main()
