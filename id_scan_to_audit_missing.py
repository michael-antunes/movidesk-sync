import os
import time
import random
import logging
import datetime as dt

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s %(message)s")
logger = logging.getLogger("sync_resolved_detail")

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").rstrip("/")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
DETAIL_TABLE = os.getenv("DETAIL_TABLE", "tickets_resolvidos_detail")
MISSING_TABLE = os.getenv("MISSING_TABLE", "audit_recent_missing")
RUN_TABLE = os.getenv("RUN_TABLE", "audit_recent_run")

MISSING_LIMIT = int(os.getenv("DETAIL_MISSING_LIMIT", "10"))
WINDOW = int(os.getenv("DETAIL_WINDOW", "2000"))
BULK_LIMIT = int(os.getenv("DETAIL_BULK_LIMIT", "200"))
MAX_ACTIONS = int(os.getenv("DETAIL_MAX_ACTIONS", os.getenv("MAX_ACTIONS", "400")))
MISSING_MAX_ATTEMPTS = int(os.getenv("DETAIL_MISSING_MAX_ATTEMPTS", "10"))
MISSING_BACKOFF_MINUTES = int(os.getenv("DETAIL_MISSING_BACKOFF_MINUTES", "10"))

PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "1.2"))

LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "5000"))
STMT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "300000"))

SELECT_META = "id,lastUpdate,baseStatus,actionCount,isDeleted"
SELECT_DETAIL = (
    "id,protocol,type,subject,category,urgency,status,baseStatus,justification,origin,"
    "createdDate,isDeleted,owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
    "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,resolvedIn,closedIn,"
    "canceledIn,actionCount,reopenedIn,lastActionDate,lastUpdate,clients,statusHistories,"
    "customFieldValues"
)

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

def request_with_retry(session, method, url, params=None):
    last = None
    for i in range(HTTP_RETRIES):
        try:
            r = session.request(method, url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"HTTP {r.status_code}: {r.text}")
                _sleep_backoff(i)
                continue
            return r
        except Exception as e:
            last = e
            _sleep_backoff(i)
    raise last

def api_get(session, path, params):
    url = f"{API_BASE}/{path.lstrip('/')}"
    params = dict(params or {})
    params["token"] = TOKEN
    return request_with_retry(session, "GET", url, params=params)

def fetch_meta(session, low_id, high_id):
    meta = {}
    skip = 0
    while True:
        r = api_get(
            session,
            "tickets",
            {
                "$select": SELECT_META,
                "$filter": f"id ge {int(low_id)} and id le {int(high_id)} and (baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')",
                "$orderby": "id desc",
                "$top": str(PAGE_SIZE),
                "$skip": str(skip),
                "includeDeletedItems": "true",
            },
        )
        if r.status_code != 200:
            break
        data = r.json()
        if not isinstance(data, list) or not data:
            break
        for row in data:
            if not isinstance(row, dict):
                continue
            try:
                tid = int(row.get("id"))
            except Exception:
                continue
            meta[tid] = row
        if len(data) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        if THROTTLE > 0:
            time.sleep(THROTTLE)

    skip = 0
    while True:
        r = api_get(
            session,
            "tickets/past",
            {
                "$select": SELECT_META,
                "$filter": f"id ge {int(low_id)} and id le {int(high_id)} and (baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')",
                "$orderby": "id desc",
                "$top": str(PAGE_SIZE),
                "$skip": str(skip),
                "includeDeletedItems": "true",
            },
        )
        if r.status_code != 200:
            break
        data = r.json()
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
            meta[tid] = row
        if len(data) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        if THROTTLE > 0:
            time.sleep(THROTTLE)

    return meta

def fetch_detail(session, ticket_id):
    r = api_get(
        session,
        f"tickets/{int(ticket_id)}",
        {"$select": SELECT_DETAIL, "includeDeletedItems": "true"},
    )
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict) and data.get("id") is not None:
            return data, 200, None
        return None, 200, "empty"
    if r.status_code != 404:
        return None, r.status_code, r.text
    r2 = api_get(
        session,
        "tickets/past",
        {"$filter": f"id eq {int(ticket_id)}", "$select": SELECT_DETAIL, "includeDeletedItems": "true"},
    )
    if r2.status_code != 200:
        return None, r2.status_code, r2.text
    data2 = r2.json()
    if isinstance(data2, dict) and "items" in data2:
        data2 = data2["items"]
    if isinstance(data2, list) and data2:
        if isinstance(data2[0], dict):
            return data2[0], 200, None
    return None, 404, "not found"

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{DETAIL_TABLE} (
              ticket_id bigint PRIMARY KEY,
              raw jsonb NOT NULL,
              updated_at timestamptz NOT NULL DEFAULT now(),
              last_update timestamptz
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{RUN_TABLE} (
              id bigserial PRIMARY KEY,
              started_at timestamptz NOT NULL DEFAULT now(),
              window_start timestamptz NOT NULL,
              window_end timestamptz NOT NULL,
              total_api integer NOT NULL,
              missing_total integer NOT NULL DEFAULT 0,
              run_at timestamptz,
              window_from timestamptz,
              window_to timestamptz,
              total_local integer,
              notes text,
              run_id bigint,
              created_at timestamptz,
              table_name text,
              external_run_id bigint,
              workflow text,
              job text,
              ref text,
              sha text,
              repo text,
              max_actions integer,
              window_start_id bigint,
              window_end_id bigint,
              window_center_id bigint
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{MISSING_TABLE} (
              run_id bigint NOT NULL DEFAULT 0,
              table_name text NOT NULL DEFAULT '{DETAIL_TABLE}',
              ticket_id integer NOT NULL,
              first_seen timestamptz NOT NULL DEFAULT now(),
              last_seen timestamptz NOT NULL DEFAULT now(),
              attempts integer NOT NULL DEFAULT 0,
              last_attempt timestamptz,
              last_status integer,
              last_error text,
              run_started_at timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (table_name, ticket_id)
            )
            """
        )
    conn.commit()

def ensure_missing_unique_index():
    idx_name = "ux_audit_recent_missing_table_ticket"
    c = psycopg2.connect(DSN)
    try:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(
                f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
                f"ON {SCHEMA}.{MISSING_TABLE} (table_name, ticket_id)"
            )
    except Exception:
        pass
    finally:
        try:
            c.close()
        except Exception:
            pass

def create_run(conn):
    ext = os.getenv("GITHUB_RUN_ID")
    ext_val = int(ext) if ext and str(ext).isdigit() else None
    now = _now_utc()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{RUN_TABLE}
              (started_at, window_start, window_end, total_api, missing_total, run_at, notes, table_name,
               external_run_id, workflow, job, ref, sha, repo, max_actions)
            VALUES
              (now(), %s, %s, 0, 0, now(), %s, %s,
               %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                now,
                now,
                "sync_resolved_detail",
                DETAIL_TABLE,
                ext_val,
                os.getenv("GITHUB_WORKFLOW"),
                os.getenv("GITHUB_JOB"),
                os.getenv("GITHUB_REF_NAME") or os.getenv("GITHUB_REF"),
                os.getenv("GITHUB_SHA"),
                os.getenv("GITHUB_REPOSITORY"),
                MAX_ACTIONS,
            ),
        )
        rid = int(cur.fetchone()[0])
    conn.commit()
    return rid

def claim_missing(conn, run_id):
    if MISSING_LIMIT <= 0:
        return []
    backoff = dt.timedelta(minutes=MISSING_BACKOFF_MINUTES)
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
        cur.execute(f"SET LOCAL statement_timeout = '{STMT_TIMEOUT_MS}ms'")
        cur.execute(
            f"""
            SELECT ticket_id
              FROM {SCHEMA}.{MISSING_TABLE}
             WHERE table_name = %s
               AND attempts < %s
               AND (last_attempt IS NULL OR last_attempt < (now() - %s::interval))
             ORDER BY last_seen ASC
             LIMIT %s
             FOR UPDATE SKIP LOCKED
            """,
            (DETAIL_TABLE, MISSING_MAX_ATTEMPTS, f"{int(backoff.total_seconds())} seconds", MISSING_LIMIT),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        if not ids:
            return []
        cur.execute(
            f"""
            UPDATE {SCHEMA}.{MISSING_TABLE}
               SET attempts = attempts + 1,
                   last_attempt = now(),
                   last_seen = now(),
                   run_id = %s,
                   run_started_at = now()
             WHERE table_name = %s AND ticket_id = ANY(%s)
            """,
            (int(run_id), DETAIL_TABLE, ids),
        )
    conn.commit()
    return ids

def upsert_detail(conn, ticket):
    tid = int(ticket.get("id"))
    lu = ticket.get("lastUpdate")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{DETAIL_TABLE} (ticket_id, raw, updated_at, last_update)
            VALUES (%s, %s, now(), %s)
            ON CONFLICT (ticket_id) DO UPDATE
              SET raw = EXCLUDED.raw,
                  updated_at = now(),
                  last_update = EXCLUDED.last_update
            RETURNING (xmax = 0) AS inserted
            """,
            (tid, psycopg2.extras.Json(ticket), lu),
        )
        inserted = bool(cur.fetchone()[0])
    conn.commit()
    return inserted

def upsert_too_big(conn, ticket_id, last_update):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{DETAIL_TABLE} (ticket_id, raw, updated_at, last_update)
            VALUES (%s, %s, now(), %s)
            ON CONFLICT (ticket_id) DO UPDATE
              SET raw = EXCLUDED.raw,
                  updated_at = now(),
                  last_update = EXCLUDED.last_update
            """,
            (int(ticket_id), psycopg2.extras.Json("Ticket muito grande"), last_update),
        )
    conn.commit()

def delete_missing(conn, ticket_id):
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {SCHEMA}.{MISSING_TABLE} WHERE table_name=%s AND ticket_id=%s",
            (DETAIL_TABLE, int(ticket_id)),
        )
    conn.commit()

def update_missing_error(conn, ticket_id, status, err):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {SCHEMA}.{MISSING_TABLE}
               SET last_seen = now(),
                   last_status = %s,
                   last_error = %s
             WHERE table_name=%s AND ticket_id=%s
            """,
            (int(status) if status is not None else None, str(err)[:4000] if err is not None else None, DETAIL_TABLE, int(ticket_id)),
        )
    conn.commit()

def window_scan(session, conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(ticket_id),0) FROM {SCHEMA}.{DETAIL_TABLE}")
        last_id = int((cur.fetchone() or [0])[0] or 0)

    if last_id <= 0:
        return 0, [], [], []

    low_id = max(1, last_id - WINDOW)
    high_id = last_id + WINDOW

    meta = fetch_meta(session, low_id, high_id)
    if not meta:
        return 0, [], [], []

    ids = sorted(meta.keys())
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT ticket_id, last_update FROM {SCHEMA}.{DETAIL_TABLE} WHERE ticket_id = ANY(%s)",
            (ids,),
        )
        db_map = {int(r[0]): r[1] for r in cur.fetchall()}

    to_fetch = []
    big = []
    for tid in sorted(meta.keys(), reverse=True):
        m = meta.get(tid) or {}
        api_lu = _parse_dt(m.get("lastUpdate"))
        db_lu = _parse_dt(db_map.get(tid))
        if api_lu is None:
            continue
        if db_lu is None or db_lu < api_lu:
            ac = m.get("actionCount")
            if isinstance(ac, int) and ac > MAX_ACTIONS:
                big.append((tid, m.get("lastUpdate")))
                continue
            to_fetch.append(tid)
        if len(to_fetch) >= BULK_LIMIT:
            break

    inserted_ids = []
    updated_ids = []
    big_ids = []

    for tid, lu in big:
        upsert_too_big(conn, tid, lu)
        big_ids.append(int(tid))

    for tid in to_fetch:
        ticket, status, err = fetch_detail(session, tid)
        if ticket is None:
            continue
        ac = ticket.get("actionCount")
        if isinstance(ac, int) and ac > MAX_ACTIONS:
            upsert_too_big(conn, tid, ticket.get("lastUpdate"))
            big_ids.append(int(tid))
            continue
        ins = upsert_detail(conn, ticket)
        if ins:
            inserted_ids.append(int(tid))
        else:
            updated_ids.append(int(tid))

    return len(to_fetch) + len(big), inserted_ids, updated_ids, big_ids

def main():
    if not TOKEN:
        raise SystemExit("MOVIDESK_TOKEN obrigatório")
    if not DSN:
        raise SystemExit("NEON_DSN obrigatório")

    conn = psycopg2.connect(DSN)
    try:
        ensure_tables(conn)
        ensure_missing_unique_index()
        run_id = create_run(conn)

        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        ids = claim_missing(conn, run_id)
        if ids:
            logger.info("missing_claim=%s", ",".join(str(x) for x in ids))
        else:
            logger.info("missing_claim=")

        inserted = []
        updated = []
        big = []
        notfound = []
        errors = []

        for tid in ids:
            try:
                ticket, status, err = fetch_detail(session, tid)
                if ticket is None:
                    if status == 404:
                        delete_missing(conn, tid)
                        notfound.append(int(tid))
                        logger.info("missing ID=%s 404 removido", tid)
                    else:
                        update_missing_error(conn, tid, status, err)
                        errors.append(int(tid))
                        logger.info("missing ID=%s erro status=%s", tid, status)
                    continue

                ac = ticket.get("actionCount")
                if isinstance(ac, int) and ac > MAX_ACTIONS:
                    upsert_too_big(conn, tid, ticket.get("lastUpdate"))
                    delete_missing(conn, tid)
                    big.append(int(tid))
                    logger.info("missing ID=%s too_big", tid)
                    continue

                ins = upsert_detail(conn, ticket)
                delete_missing(conn, tid)

                if ins:
                    inserted.append(int(tid))
                    logger.info("missing ID=%s inserido", tid)
                else:
                    updated.append(int(tid))
                    logger.info("missing ID=%s atualizado", tid)
            except Exception as e:
                update_missing_error(conn, tid, None, f"{type(e).__name__}: {e}")
                errors.append(int(tid))
                logger.info("missing ID=%s erro=%s", tid, type(e).__name__)

        c, ins2, upd2, big2 = window_scan(session, conn)
        if c:
            logger.info("janela processados=%s inseridos=%s atualizados=%s too_big=%s", c, len(ins2), len(upd2), len(big2))
            if upd2:
                logger.info("janela_atualizados=%s", ",".join(str(x) for x in upd2[:200]))
            if ins2:
                logger.info("janela_inseridos=%s", ",".join(str(x) for x in ins2[:200]))
            if big2:
                logger.info("janela_toobig=%s", ",".join(str(x) for x in big2[:200]))

        if updated:
            logger.info("missing_atualizados=%s", ",".join(str(x) for x in updated[:200]))
        if inserted:
            logger.info("missing_inseridos=%s", ",".join(str(x) for x in inserted[:200]))
        if big:
            logger.info("missing_toobig=%s", ",".join(str(x) for x in big[:200]))
        if notfound:
            logger.info("missing_404=%s", ",".join(str(x) for x in notfound[:200]))
        if errors:
            logger.info("missing_erros=%s", ",".join(str(x) for x in errors[:200]))

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
