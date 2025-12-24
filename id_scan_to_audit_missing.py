import os
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
import psycopg2
from psycopg2.extras import execute_values


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

TABLE_NAME = "tickets_resolvidos"
BASE_STATUSES = {"Resolved", "Closed", "Canceled"}

BATCH_SIZE = int(os.getenv("ID_VALIDATE_BATCH_SIZE", "200"))
MAX_RUNTIME_SEC = int(os.getenv("ID_VALIDATE_MAX_RUNTIME_SEC", "240"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "60"))

START_ID_ENV = os.getenv("ID_VALIDATE_START_ID")
END_ID = int(os.getenv("ID_VALIDATE_END_ID", "1"))

SELECT_MIN = "id,baseStatus,isDeleted,lastUpdate"


def pg_conn():
    return psycopg2.connect(DSN)


def ensure_control(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.id_validate_api_control(
          id_cursor bigint not null
        )
        """
    )
    cur.execute("select count(*) from visualizacao_resolvidos.id_validate_api_control")
    if cur.fetchone()[0] == 0:
        cur.execute("insert into visualizacao_resolvidos.id_validate_api_control(id_cursor) values (0)")


def get_max_known_id(cur):
    cur.execute(
        """
        select greatest(
          coalesce((select max(ticket_id) from visualizacao_resolvidos.tickets_resolvidos_detail),0),
          coalesce((select max(ticket_id::bigint) from visualizacao_atual.tickets_abertos),0),
          coalesce((select max(greatest(ticket_id, merged_into_id)) from visualizacao_resolvidos.tickets_mesclados),0)
        )
        """
    )
    return int(cur.fetchone()[0] or 0)


def get_cursor(cur):
    cur.execute("select id_cursor from visualizacao_resolvidos.id_validate_api_control limit 1")
    return int(cur.fetchone()[0] or 0)


def set_cursor(cur, v):
    cur.execute("update visualizacao_resolvidos.id_validate_api_control set id_cursor=%s", (int(v),))


def create_run(cur, notes):
    cur.execute(
        """
        insert into visualizacao_resolvidos.audit_recent_run
          (window_start, window_end, total_api, missing_total, run_at, notes)
        values (now(), now(), 0, 0, now(), %s)
        returning id
        """,
        (notes,),
    )
    return int(cur.fetchone()[0])


def update_run(cur, run_id, total_api, missing_total, total_local):
    cur.execute(
        """
        update visualizacao_resolvidos.audit_recent_run
           set window_end = now(),
               total_api = %s,
               missing_total = %s,
               total_local = %s
         where id = %s
        """,
        (int(total_api), int(missing_total), int(total_local), int(run_id)),
    )


def fetch_skip_sets(cur, ids):
    cur.execute(
        "select ticket_id from visualizacao_resolvidos.tickets_resolvidos_detail where ticket_id = any(%s)",
        (ids,),
    )
    in_detail = {int(r[0]) for r in cur.fetchall()}

    cur.execute(
        "select ticket_id::bigint from visualizacao_atual.tickets_abertos where ticket_id::bigint = any(%s)",
        (ids,),
    )
    in_abertos = {int(r[0]) for r in cur.fetchall()}

    cur.execute(
        """
        select ticket_id
          from visualizacao_resolvidos.tickets_mesclados
         where ticket_id = any(%s) or merged_into_id = any(%s)
        """,
        (ids, ids),
    )
    in_mesclados = {int(r[0]) for r in cur.fetchall()}

    cur.execute(
        """
        select ticket_id
          from visualizacao_resolvidos.audit_recent_missing
         where table_name = %s
           and ticket_id = any(%s)
        """,
        (TABLE_NAME, ids),
    )
    in_missing = {int(r[0]) for r in cur.fetchall()}

    return in_detail, in_abertos, in_mesclados, in_missing


def http_get(session, url, params):
    last = None
    for i in range(1, ATTEMPTS + 1):
        try:
            r = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(10, 2 ** (i - 1)))
                continue
            return r
        except Exception as e:
            last = e
            time.sleep(min(10, 2 ** (i - 1)))
    raise last if last else RuntimeError("http failed")


def api_check_ticket(session, ticket_id):
    p = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$select": SELECT_MIN,
        "id": str(int(ticket_id)),
    }
    r = http_get(session, f"{BASE}/tickets", p)
    if r.status_code == 200:
        j = r.json()
        if isinstance(j, dict) and j.get("id") is not None:
            return True, str(j.get("baseStatus") or "")
        return False, ""
    if r.status_code != 404:
        return False, ""

    p2 = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$select": SELECT_MIN,
        "$filter": f"id eq {int(ticket_id)}",
    }
    r2 = http_get(session, f"{BASE}/tickets/past", p2)
    if r2.status_code == 200:
        j2 = r2.json()
        if isinstance(j2, list) and len(j2) > 0 and isinstance(j2[0], dict) and j2[0].get("id") is not None:
            return True, str(j2[0].get("baseStatus") or "")
        return False, ""
    return False, ""


def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        """
        insert into visualizacao_resolvidos.audit_recent_missing
          (run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        [(int(run_id), TABLE_NAME, int(i)) for i in ids],
    )
    return len(ids)


def main():
    if not DSN or not TOKEN:
        raise RuntimeError("NEON_DSN e MOVIDESK_TOKEN são obrigatórios")

    with pg_conn() as conn, conn.cursor() as cur:
        ensure_control(cur)

        if START_ID_ENV:
            start_id = int(START_ID_ENV)
        else:
            start_id = get_max_known_id(cur)

        cursor = get_cursor(cur)
        if cursor <= 0 or cursor > start_id:
            cursor = start_id
            set_cursor(cur, cursor)

        run_id = create_run(cur, f"id-validate-api: range {cursor}->{END_ID} | skip detail/abertos/mesclados | insert audit_missing")
        conn.commit()

        session = requests.Session()

        t0 = time.time()
        total_scanned = 0
        total_api = 0
        total_insert = 0

        while cursor >= END_ID and (time.time() - t0) < MAX_RUNTIME_SEC:
            batch_end = max(END_ID, cursor - BATCH_SIZE + 1)
            ids = list(range(cursor, batch_end - 1, -1))
            total_scanned += len(ids)

            in_detail, in_abertos, in_mesclados, in_missing = fetch_skip_sets(cur, ids)
            skip = in_detail | in_abertos | in_mesclados | in_missing
            candidates = [i for i in ids if i not in skip]

            valid = []
            for tid in candidates:
                ok, base_status = api_check_ticket(session, tid)
                total_api += 1
                if ok and base_status in BASE_STATUSES:
                    valid.append(tid)
                if THROTTLE > 0:
                    time.sleep(THROTTLE)

            total_insert += insert_missing(cur, run_id, valid)
            cursor = batch_end - 1
            set_cursor(cur, cursor)
            conn.commit()

        update_run(cur, run_id, total_api, total_insert, total_scanned)
        conn.commit()

        logging.info(
            "Finalizado. scanned=%s api_checked=%s inserted=%s cursor=%s",
            total_scanned,
            total_api,
            total_insert,
            cursor,
        )


if __name__ == "__main__":
    main()
