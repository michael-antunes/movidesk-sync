import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN", "").strip()
NEON_DSN = os.getenv("NEON_DSN", "").strip()

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos").strip()
AUDIT_TABLE = os.getenv("AUDIT_TABLE", "audit_recent_missing").strip()

TABLE_NAME_RAW = os.getenv("TABLE_NAME", "tickets_resolvidos").strip()
TABLE_NAME = "tickets_resolvidos" if TABLE_NAME_RAW == "tickets_resolvidos_detail" else TABLE_NAME_RAW

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").strip()

START_ID = int(os.getenv("START_ID", "1"))
BATCH = int(os.getenv("BATCH", "200"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.2"))

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "45"))
LIMIT_IDS = int(os.getenv("LIMIT_IDS", "3000"))

if not MOVIDESK_TOKEN:
    raise SystemExit("MOVIDESK_TOKEN não definido")
if not NEON_DSN:
    raise SystemExit("NEON_DSN não definido")


def pg_connect():
    return psycopg2.connect(NEON_DSN)


def ensure_audit_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{AUDIT_TABLE} (
      run_id BIGINT NOT NULL,
      table_name TEXT NOT NULL,
      ticket_id BIGINT NOT NULL,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (run_id, table_name, ticket_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def get_recent_resolved_ids():
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    url = f"{API_BASE}/tickets"
    params = {
        "token": MOVIDESK_TOKEN,
        "$select": "id,resolutionDate",
        "$filter": f"resolutionDate ge {since.isoformat()}",
        "$orderby": "id asc",
        "$top": 1000,
    }

    ids = []
    skip = 0
    while True:
        params["$skip"] = skip
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json() or []
        if not data:
            break
        for t in data:
            tid = t.get("id")
            if isinstance(tid, int):
                ids.append(tid)
        if len(data) < 1000:
            break
        skip += 1000
        time.sleep(SLEEP_SEC)

        if LIMIT_IDS and len(ids) >= LIMIT_IDS:
            ids = ids[:LIMIT_IDS]
            break

    return ids


def ids_in_detail(conn, ids):
    if not ids:
        return set()
    placeholders = ",".join(["%s"] * len(ids))
    sql = f"""
      SELECT ticket_id
      FROM {SCHEMA}.tickets_resolvidos_detail
      WHERE ticket_id IN ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(sql, ids)
        return {row[0] for row in cur.fetchall()}


def insert_missing(conn, run_id, missing_ids):
    if not missing_ids:
        return 0
    rows = [(run_id, TABLE_NAME, tid) for tid in missing_ids]
    sql = f"""
      INSERT INTO {SCHEMA}.{AUDIT_TABLE} (run_id, table_name, ticket_id)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    run_id = int(datetime.now(timezone.utc).timestamp())
    logging.info("run_id=%s table_name(normalizado)=%s", run_id, TABLE_NAME)

    with pg_connect() as conn:
        ensure_audit_table(conn)

        logging.info("buscando ids resolvidos recentes na API (lookback=%sd)", LOOKBACK_DAYS)
        recent_ids = get_recent_resolved_ids()
        logging.info("ids recentes obtidos=%s", len(recent_ids))

        existing = ids_in_detail(conn, recent_ids)
        missing = [tid for tid in recent_ids if tid not in existing]
        logging.info("presentes_no_detail=%s missing=%s", len(existing), len(missing))

        up = insert_missing(conn, run_id, missing)
        logging.info("audit_missing_upsert=%s", up)


if __name__ == "__main__":
    main()
import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MOVIDESK_TOKEN = os.getenv("MOVIDESK_TOKEN", "").strip()
NEON_DSN = os.getenv("NEON_DSN", "").strip()

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos").strip()
AUDIT_TABLE = os.getenv("AUDIT_TABLE", "audit_recent_missing").strip()

TABLE_NAME_RAW = os.getenv("TABLE_NAME", "tickets_resolvidos").strip()
TABLE_NAME = "tickets_resolvidos" if TABLE_NAME_RAW == "tickets_resolvidos_detail" else TABLE_NAME_RAW

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").strip()

START_ID = int(os.getenv("START_ID", "1"))
BATCH = int(os.getenv("BATCH", "200"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.2"))

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "45"))
LIMIT_IDS = int(os.getenv("LIMIT_IDS", "3000"))

if not MOVIDESK_TOKEN:
    raise SystemExit("MOVIDESK_TOKEN não definido")
if not NEON_DSN:
    raise SystemExit("NEON_DSN não definido")


def pg_connect():
    return psycopg2.connect(NEON_DSN)


def ensure_audit_table(conn):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{AUDIT_TABLE} (
      run_id BIGINT NOT NULL,
      table_name TEXT NOT NULL,
      ticket_id BIGINT NOT NULL,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (run_id, table_name, ticket_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def get_recent_resolved_ids():
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    url = f"{API_BASE}/tickets"
    params = {
        "token": MOVIDESK_TOKEN,
        "$select": "id,resolutionDate",
        "$filter": f"resolutionDate ge {since.isoformat()}",
        "$orderby": "id asc",
        "$top": 1000,
    }

    ids = []
    skip = 0
    while True:
        params["$skip"] = skip
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json() or []
        if not data:
            break
        for t in data:
            tid = t.get("id")
            if isinstance(tid, int):
                ids.append(tid)
        if len(data) < 1000:
            break
        skip += 1000
        time.sleep(SLEEP_SEC)

        if LIMIT_IDS and len(ids) >= LIMIT_IDS:
            ids = ids[:LIMIT_IDS]
            break

    return ids


def ids_in_detail(conn, ids):
    if not ids:
        return set()
    placeholders = ",".join(["%s"] * len(ids))
    sql = f"""
      SELECT ticket_id
      FROM {SCHEMA}.tickets_resolvidos_detail
      WHERE ticket_id IN ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(sql, ids)
        return {row[0] for row in cur.fetchall()}


def insert_missing(conn, run_id, missing_ids):
    if not missing_ids:
        return 0
    rows = [(run_id, TABLE_NAME, tid) for tid in missing_ids]
    sql = f"""
      INSERT INTO {SCHEMA}.{AUDIT_TABLE} (run_id, table_name, ticket_id)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    run_id = int(datetime.now(timezone.utc).timestamp())
    logging.info("run_id=%s table_name(normalizado)=%s", run_id, TABLE_NAME)

    with pg_connect() as conn:
        ensure_audit_table(conn)

        logging.info("buscando ids resolvidos recentes na API (lookback=%sd)", LOOKBACK_DAYS)
        recent_ids = get_recent_resolved_ids()
        logging.info("ids recentes obtidos=%s", len(recent_ids))

        existing = ids_in_detail(conn, recent_ids)
        missing = [tid for tid in recent_ids if tid not in existing]
        logging.info("presentes_no_detail=%s missing=%s", len(existing), len(missing))

        up = insert_missing(conn, run_id, missing)
        logging.info("audit_missing_upsert=%s", up)


if __name__ == "__main__":
    main()
