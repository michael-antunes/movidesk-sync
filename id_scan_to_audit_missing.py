import os
import time
import random
import logging
import datetime as dt

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import errors

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").rstrip("/")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos_detail")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "1000"))
LOOPS = int(os.getenv("ID_SCAN_ITERATIONS", "200000"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
TIMEOUT = int(os.getenv("TIMEOUT", "60"))

LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "5000"))
STMT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "300000"))
UPSERT_PAGE_SIZE = int(os.getenv("DB_UPSERT_PAGE_SIZE", "200"))
TXN_RETRIES = int(os.getenv("DB_TXN_RETRIES", "6"))

HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "1.2"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("id_scan_to_audit_missing")

def _now_utc():
    return dt.datetime.now(dt.timezone.utc)

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

def _sleep_backoff(i):
    time.sleep(HTTP_BACKOFF * (i + 1) + random.random() * 0.2)

def api_get(session, path, params):
    url = f"{API_BASE}/{path.lstrip('/')}"
    params = dict(params or {})
    params["token"] = TOKEN
    last = None
    for i in range(HTTP_RETRIES):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"HTTP {r.status_code}: {r.text}")
                _sleep_backoff(i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            _sleep_backoff(i)
    raise last

def api_list_meta(session, endpoint, low_id, high_id):
    out = {}
    skip = 0
    flt = (
        f"id ge {int(low_id)} and id le {int(high_id)} and "
        f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')"
    )
    while True:
        data = api_get(
            session,
            endpoint,
            {
                "$select": "id,lastUpdate,baseStatus",
                "$filter": flt,
                "$top": str(TOP),
                "$skip": str(skip),
                "includeDeletedItems": "true",
            },
        )
        if isinstance(data, dict) and "items" in data:
            data = data["items"]
        if not isinstance(data, list) or not data:
            break
        for row in data:
            if not isinstance(row, dict):
                continue
            try:
                tid = int(row.get("id"))
            except Exception:
                continue
            out[tid] = row.get("lastUpdate")
        if len(data) < TOP:
            break
        skip += TOP
        if THROTTLE > 0:
            time.sleep(THROTTLE)
    return out

def db_conn():
    return psycopg2.connect(DSN)

def ensure_missing_unique_index():
    idx_name = "ux_audit_recent_missing_table_ticket"
    c = db_conn()
    try:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(
                f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
                f"ON {SCHEMA}.audit_recent_missing (table_name, ticket_id)"
            )
    except Exception:
        pass
    finally:
        try:
            c.close()
        except Exception:
            pass

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
            c = db_conn()
            with c:
                with c.cursor() as cur:
                    cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
                    cur.execute(f"SET LOCAL statement_timeout = '{STMT_TIMEOUT_MS}ms'")
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

def create_run(cur):
    ext = os.getenv("GITHUB_RUN_ID")
    ext_val = int(ext) if ext and str(ext).isdigit() else None
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.audit_recent_run
          (started_at, window_start, window_end, total_api, missing_total, run_at, notes, table_name,
           external_run_id, workflow, job, ref, sha, repo)
        VALUES
          (now(), now(), now(), 0, 0, now(), %s, %s,
           %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            "id_scan",
            TABLE_NAME,
            ext_val,
            os.getenv("GITHUB_WORKFLOW"),
            os.getenv("GITHUB_JOB"),
            os.getenv("GITHUB_REF_NAME") or os.getenv("GITHUB_REF"),
            os.getenv("GITHUB_SHA"),
            os.getenv("GITHUB_REPOSITORY"),
        ),
    )
    return int(cur.fetchone()[0])

def update_run(cur, run_id, total_api, missing_total):
    cur.execute(
        f"""
        UPDATE {SCHEMA}.audit_recent_run
           SET window_end = now(),
               total_api = %s,
               missing_total = %s
         WHERE id = %s
        """,
        (int(total_api), int(missing_total), int(run_id)),
    )

def read_control(cur):
    cur.execute(f"SELECT id_inicial, id_final, id_atual FROM {SCHEMA}.range_scan_control LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"{SCHEMA}.range_scan_control sem linhas")
    id_inicial, id_final, id_atual = row
    if id_inicial is None or id_final is None:
        raise RuntimeError(f"{SCHEMA}.range_scan_control precisa de id_inicial (maior) e id_final (menor)")
    high_bound = int(id_inicial)
    low_bound = int(id_final)

    if high_bound < low_bound:
        raise RuntimeError("range_scan_control inválido: id_inicial (maior) < id_final (menor)")

    if id_atual is None:
        cursor = high_bound
    else:
        cursor = int(id_atual)

    if cursor > high_bound:
        cursor = high_bound
    if cursor < low_bound:
        cursor = high_bound

    return high_bound, low_bound, cursor

def set_cursor(cur, new_cursor):
    cur.execute(f"UPDATE {SCHEMA}.range_scan_control SET id_atual=%s", (int(new_cursor),))

def upsert_missing(cur, run_id, ids):
    if not ids:
        return 0
    now = _now_utc()
    rows = [(int(run_id), TABLE_NAME, int(tid), now, now, 0, now) for tid in sorted(ids)]
    sql = (
        f"INSERT INTO {SCHEMA}.audit_recent_missing "
        f"(run_id, table_name, ticket_id, first_seen, last_seen, attempts, run_started_at) "
        f"VALUES %s "
        f"ON CONFLICT (table_name, ticket_id) DO UPDATE SET "
        f"last_seen=EXCLUDED.last_seen, run_id=EXCLUDED.run_id"
    )
    psycopg2.extras.execute_values(cur, sql, rows, page_size=UPSERT_PAGE_SIZE)
    return len(rows)

def main():
    if not TOKEN:
        raise SystemExit("MOVIDESK_TOKEN obrigatório")
    if not DSN:
        raise SystemExit("NEON_DSN obrigatório")

    ensure_missing_unique_index()

    run_id = run_db_txn(create_run)
    high_bound, low_bound, cursor = run_db_txn(read_control)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    api_total = 0
    missing_upserts = 0
    loops = 0

    logger.info("control high=%s low=%s cursor=%s batch=%s", high_bound, low_bound, cursor, BATCH_SIZE)

    while loops < LOOPS and cursor >= low_bound:
        loops += 1
        high_id = cursor
        low_id = max(low_bound, cursor - BATCH_SIZE + 1)

        meta = {}
        meta.update(api_list_meta(session, "tickets", low_id, high_id))
        meta.update(api_list_meta(session, "tickets/past", low_id, high_id))

        api_ids = set(meta.keys())
        api_total += len(api_ids)

        def work(cur):
            cur.execute(
                f"SELECT ticket_id, last_update FROM {SCHEMA}.tickets_resolvidos_detail WHERE ticket_id BETWEEN %s AND %s",
                (low_id, high_id),
            )
            detail_rows = cur.fetchall()
            in_detail = {int(r[0]) for r in detail_rows}
            detail_map = {int(r[0]): r[1] for r in detail_rows}

            cur.execute(
                "SELECT ticket_id FROM visualizacao_atual.tickets_abertos WHERE ticket_id BETWEEN %s AND %s",
                (low_id, high_id),
            )
            in_abertos = {int(r[0]) for r in cur.fetchall()}

            cur.execute(
                f"SELECT ticket_id FROM {SCHEMA}.tickets_mesclados WHERE ticket_id BETWEEN %s AND %s",
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
                api_dt = _parse_dt(meta.get(tid))
                db_dt = _parse_dt(detail_map.get(tid))
                if api_dt is None:
                    continue
                if db_dt is None or db_dt < api_dt:
                    stale.add(tid)

            missing = set(missing_new) | set(stale)
            upserted = upsert_missing(cur, run_id, missing)

            new_cursor = low_id - 1
            set_cursor(cur, new_cursor)

            new_sample = sorted(list(missing_new))[:20]
            stale_sample = sorted(list(stale))[:20]
            return upserted, len(missing_new), len(stale), new_sample, stale_sample, new_cursor

        upserted, c_new, c_stale, new_sample, stale_sample, new_cursor = run_db_txn(work)
        missing_upserts += upserted

        msg = f"range={low_id}..{high_id} api={len(api_ids)} upsert_missing={upserted} novos={c_new} atualizacao={c_stale} cursor->{new_cursor}"
        if new_sample:
            msg += f" novos_ids={','.join(str(x) for x in new_sample)}"
        if stale_sample:
            msg += f" stale_ids={','.join(str(x) for x in stale_sample)}"
        logger.info(msg)

        cursor = new_cursor
        if cursor < low_bound:
            break

    run_db_txn(lambda cur: update_run(cur, run_id, api_total, missing_upserts))
    logger.info("done run_id=%s total_api=%s missing_upserts=%s", run_id, api_total, missing_upserts)

if __name__ == "__main__":
    main()
