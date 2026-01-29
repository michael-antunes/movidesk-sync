import os
import sys
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg2
from psycopg2.extras import Json

API_BASE = "https://api.movidesk.com/public/v1"


def mask_token(url):
    try:
        p = urlparse(url)
        qs = parse_qsl(p.query, keep_blank_values=True)
        qs2 = []
        for k, v in qs:
            if k.lower() == "token":
                qs2.append((k, "***"))
            else:
                qs2.append((k, v))
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs2, doseq=True), p.fragment))
    except Exception:
        return url


def env_int(name, default):
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def setup_logger():
    logger = logging.getLogger("movidesk-detail")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(logging.INFO)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    if not logger.handlers:
        logger.addHandler(h)
    return logger


def pg_connect(dsn):
    return psycopg2.connect(
        dsn,
        connect_timeout=env_int("PGCONNECT_TIMEOUT", 15),
        application_name=os.getenv("PGAPPNAME", "movidesk-detail"),
    )


def pg_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchone()


def pg_exec(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])


def request_with_retry(session, logger, url, params, attempts, timeout):
    last = None
    for i in range(1, attempts + 1):
        try:
            req = requests.Request("GET", url, params=params)
            prepped = session.prepare_request(req)
            resp = session.send(prepped, timeout=timeout)
            logger.info(f"GET {mask_token(prepped.url)} -> {resp.status_code} (attempt {i}/{attempts})")
            if resp.status_code in (408, 429, 500, 502, 503, 504):
                if i < attempts:
                    time.sleep(min(30, 2 ** (i - 1)))
                    continue
            return resp
        except Exception as e:
            last = e
            logger.info(f"GET {mask_token(url)} -> EXC (attempt {i}/{attempts}): {type(e).__name__}: {e}")
            if i < attempts:
                time.sleep(min(30, 2 ** (i - 1)))
                continue
            raise
    if last:
        raise last


def fetch_ticket_detail(session, logger, token, ticket_id, attempts, timeout):
    params = {"id": str(ticket_id), "includeDeletedItems": "true", "token": token}
    r = request_with_retry(session, logger, f"{API_BASE}/tickets", params, attempts, timeout)
    if r.status_code == 200:
        try:
            data = r.json()
        except Exception:
            return None, r.status_code, r.text
        if isinstance(data, dict):
            return data, 200, None
        if isinstance(data, list) and data:
            return data[0], 200, None
        return None, 200, None
    if r.status_code != 404:
        try:
            return None, r.status_code, r.text
        except Exception:
            return None, r.status_code, None

    params2 = {"$filter": f"id eq {int(ticket_id)}", "includeDeletedItems": "true", "token": token}
    r2 = request_with_retry(session, logger, f"{API_BASE}/tickets/past", params2, attempts, timeout)
    if r2.status_code == 200:
        try:
            data = r2.json()
        except Exception:
            return None, 200, r2.text
        if isinstance(data, dict):
            return data, 200, None
        if isinstance(data, list) and data:
            return data[0], 200, None
        return None, 200, None
    try:
        return None, r2.status_code, r2.text
    except Exception:
        return None, r2.status_code, None


def pick_next_missing(conn, days):
    row = pg_one(
        conn,
        """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where last_seen >= now() - (%s * interval '1 day')
        order by last_seen desc, ticket_id desc
        limit 1
        for update skip locked
        """,
        [int(days)],
    )
    if not row:
        return None
    ticket_id = int(row[0])
    pg_exec(
        conn,
        """
        update visualizacao_resolvidos.audit_recent_missing
        set attempts = coalesce(attempts,0) + 1,
            last_attempt = now()
        where ticket_id = %s
        """,
        [ticket_id],
    )
    return ticket_id


def update_missing_error(conn, ticket_id, status_code, err_text):
    pg_exec(
        conn,
        """
        update visualizacao_resolvidos.audit_recent_missing
        set last_seen = now(),
            last_status = %s,
            last_error = %s
        where ticket_id = %s
        """,
        [status_code, (err_text or "")[:4000] if err_text else None, int(ticket_id)],
    )


def delete_missing(conn, ticket_id):
    pg_exec(
        conn,
        "delete from visualizacao_resolvidos.audit_recent_missing where ticket_id=%s",
        [int(ticket_id)],
    )


def upsert_detail(conn, ticket_id, raw_obj, last_update_dt):
    pg_exec(
        conn,
        """
        insert into visualizacao_resolvidos.tickets_resolvidos_detail (ticket_id, raw, updated_at, last_update)
        values (%s, %s, now(), %s)
        on conflict (ticket_id) do update
        set raw = excluded.raw,
            updated_at = now(),
            last_update = excluded.last_update
        """,
        [int(ticket_id), Json(raw_obj), last_update_dt],
    )


def main():
    logger = setup_logger()

    dsn = os.getenv("NEON_DSN")
    token = os.getenv("MOVIDESK_TOKEN")
    if not dsn or not token:
        raise SystemExit("NEON_DSN e MOVIDESK_TOKEN são obrigatórios")

    days = env_int("DETAIL_MISSING_DAYS", 2)
    max_to_process = env_int("DETAIL_MISSING_LIMIT", 50)
    api_attempts = env_int("DETAIL_API_ATTEMPTS", 4)
    connect_timeout = env_int("DETAIL_CONNECT_TIMEOUT", 10)
    read_timeout = env_int("DETAIL_READ_TIMEOUT", 120)
    timeout = (connect_timeout, read_timeout)

    session = requests.Session()

    updated = []
    failed = []

    processed = 0
    while processed < max_to_process:
        conn = pg_connect(dsn)
        try:
            conn.autocommit = False
            ticket_id = pick_next_missing(conn, days)
            if ticket_id is None:
                conn.commit()
                break
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

        ticket, sc, err = fetch_ticket_detail(session, logger, token, ticket_id, api_attempts, timeout)
        if ticket is None:
            conn2 = pg_connect(dsn)
            try:
                conn2.autocommit = False
                update_missing_error(conn2, ticket_id, sc, err or "ticket_none")
                conn2.commit()
            except Exception:
                try:
                    conn2.rollback()
                except Exception:
                    pass
            finally:
                try:
                    conn2.close()
                except Exception:
                    pass
            failed.append(ticket_id)
            processed += 1
            continue

        lu = ticket.get("lastUpdate") or ticket.get("lastUpdateDate") or ticket.get("lastUpdateAt")
        last_update_dt = parse_ts(lu)

        conn3 = pg_connect(dsn)
        try:
            conn3.autocommit = False
            upsert_detail(conn3, ticket_id, ticket, last_update_dt)
            delete_missing(conn3, ticket_id)
            conn3.commit()
            updated.append(ticket_id)
        except Exception as e:
            try:
                conn3.rollback()
            except Exception:
                pass
            conn4 = pg_connect(dsn)
            try:
                conn4.autocommit = False
                update_missing_error(conn4, ticket_id, None, f"db_upsert_detail: {type(e).__name__}: {e}")
                conn4.commit()
            except Exception:
                try:
                    conn4.rollback()
                except Exception:
                    pass
            finally:
                try:
                    conn4.close()
                except Exception:
                    pass
            failed.append(ticket_id)
        finally:
            try:
                conn3.close()
            except Exception:
                pass

        processed += 1

    if updated:
        logger.info("tickets_atualizados=" + ",".join(str(x) for x in updated))
    if failed:
        logger.info("tickets_falha=" + ",".join(str(x) for x in failed))
    logger.info(f"processados={processed} atualizados={len(updated)} falhas={len(failed)}")


if __name__ == "__main__":
    main()
