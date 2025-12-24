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

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")


def to_utc(d):
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).replace(microsecond=0)


def iso_z(d):
    d = to_utc(d)
    return d.isoformat().replace("+00:00", "Z")


def parse_dt(s):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        return None


def conn():
    return psycopg2.connect(DSN)


def ensure_table():
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            create table if not exists visualizacao_resolvidos.range_scan_control(
              data_fim timestamptz not null,
              data_inicio timestamptz not null,
              ultima_data_validada timestamptz,
              constraint ck_range_scan_bounds check(
                data_inicio is not null and data_fim is not null and data_inicio <> data_fim
                and (
                  ultima_data_validada is null
                  or (
                    ultima_data_validada >= least(data_inicio,data_fim)
                    and ultima_data_validada <= greatest(data_inicio,data_fim)
                  )
                )
              )
            )
            """
        )
        cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
        n = cur.fetchone()[0]
        if n == 0:
            cur.execute(
                """
                insert into visualizacao_resolvidos.range_scan_control(data_fim, data_inicio, ultima_data_validada)
                values (now(), timestamptz '2018-01-01 00:00:00+00', now())
                """
            )
        c.commit()


def mesclados_last_updates(cur, ids):
    if not ids:
        return {}
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'visualizacao_resolvidos'
          and table_name = 'tickets_mesclados'
        """
    )
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        return {}
    lu_col = None
    for c in cols:
        lc = c.lower()
        if lc in ("last_update", "lastupdate", "updated_at", "last_updated_at", "lastupdateat"):
            lu_col = c
            break
    if lu_col is None:
        for c in cols:
            lc = c.lower()
            if "last" in lc and "update" in lc:
                lu_col = c
                break
    if lu_col is None:
        return {}
    try:
        cur.execute(
            f"select ticket_id, {lu_col} from visualizacao_resolvidos.tickets_mesclados where ticket_id = any(%s)",
            (ids,),
        )
    except Exception:
        return {}
    out = {}
    for tid, lu in cur.fetchall():
        if tid is None or lu is None:
            continue
        out[int(tid)] = lu
    return out


def fetch_page(params):
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.status_code} {r.text[:300]}")
    data = r.json()
    return data or []


def do_one_cycle():
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio)
            from visualizacao_resolvidos.range_scan_control
            limit 1
            """
        )
        row = cur.fetchone()
        data_inicio, data_fim, ultima = row[0], row[1], row[2]

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
            lu = parse_dt(t.get("lastUpdate"))
            if not lu:
                continue
            if lu < data_fim or lu > ultima:
                continue
            try:
                tid = int(t.get("id"))
            except Exception:
                continue
            if tid not in ids:
                ids.append(tid)
                api_last[tid] = lu
                if min_lu is None or lu < min_lu:
                    min_lu = lu

        if len(ids) >= LIMIT:
            break
        if len(page) < PAGE_TOP:
            break

        skip += PAGE_TOP
        time.sleep(THROTTLE)

    missing = []

    with conn() as c, c.cursor() as cur:
        if ids:
            cur.execute(
                """
                select ticket_id, last_update
                from visualizacao_resolvidos.tickets_resolvidos_detail
                where ticket_id = any(%s)
                """,
                (ids,),
            )
            rows = cur.fetchall()
            db_last = {int(r[0]): r[1] for r in rows}

            mesclados_last = mesclados_last_updates(cur, ids)

            for tid in ids:
                api_dt = to_utc(api_last.get(tid))
                m_dt = mesclados_last.get(tid)
                if m_dt is not None and api_dt is not None:
                    if to_utc(m_dt) >= api_dt:
                        continue
                db_dt = db_last.get(tid)
                if db_dt is None:
                    missing.append(tid)
                else:
                    if to_utc(db_dt) != api_dt:
                        missing.append(tid)

            cur.execute("select max(id) from visualizacao_resolvidos.audit_recent_run")
            rid = cur.fetchone()[0]
            if rid is None:
                cur.execute(
                    """
                    insert into visualizacao_resolvidos.audit_recent_run(table_name, range_count, api_ids, inserted_missing)
                    values (%s, 0, 0, 0)
                    returning id
                    """,
                    ("tickets_resolvidos",),
                )
                rid = cur.fetchone()[0]

            if missing:
                execute_values(
                    cur,
                    """
                    insert into visualizacao_resolvidos.audit_recent_missing
                      (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                    """,
                    [(rid, "tickets_resolvidos", m) for m in missing],
                )

        if min_lu is not None:
            nv = min_lu
        else:
            nv = data_fim

        cur.execute(
            """
            update visualizacao_resolvidos.range_scan_control
               set ultima_data_validada = %s
            """,
            (nv,),
        )
        c.commit()

    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            select (coalesce(ultima_data_validada, data_inicio) <= data_fim)
            from visualizacao_resolvidos.range_scan_control
            limit 1
            """
        )
        hit_end = bool(cur.fetchone()[0])

    return hit_end, len(ids)


def main():
    ensure_table()
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
