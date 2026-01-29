import os
import sys
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg2
from psycopg2.extras import Json


API_BASE_DEFAULT = "https://api.movidesk.com/public/v1"


def mask_token(url: str) -> str:
    try:
        p = urlparse(url)
        qs = parse_qsl(p.query, keep_blank_values=True)
        qs2 = []
        for k, v in qs:
            if k.lower() in ("token", "movidesk_token", "movidesk_api_token"):
                qs2.append((k, "***"))
            else:
                qs2.append((k, v))
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs2, doseq=True), p.fragment))
    except Exception:
        return url


def env_int(name: str, default: int) -> int:
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
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
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


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        connect_timeout=env_int("PGCONNECT_TIMEOUT", 15),
        application_name=os.getenv("PGAPPNAME", "movidesk-detail"),
    )


def pg_one(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchone()


def pg_exec(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])


def request_with_retry(session, logger, url, params, attempts, timeout):
    last_exc = None
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
            last_exc = e
            logger.info(f"GET {mask_token(url)} -> EXC (attempt {i}/{attempts}): {type(e).__name__}: {e}")
            if i < attempts:
                time.sleep(min(30, 2 ** (i - 1)))
                continue
            raise
    if last_exc:
        raise last_exc


def fetch_ticket_detail(session, logger, api_base, token, ticket_id, attempts, timeout):
    params = {"id": str(ticket_id), "includeDeletedItems": "true", "token": token}
    r = request_with_retry(session, logger, f"{api_base}/tickets", params, attempts, timeout)
    if r.status_code == 200:
        try:
            data = r.json()
        except Exception:
            return None, 200, "json_parse_error"
        if isinstance(data, dict):
            return data, 200, None
        if isinstance(data, list) and data:
            return data[0], 200, None
        return None, 200, "empty_payload"

    if r.status_code != 404:
        try:
            return None, r.status_code, r.text
        except Exception:
            return None, r.status_code, "http_error"

    params2 = {"$filter": f"id eq {int(ticket_id)}", "includeDeletedItems": "true", "token": token}
    r2 = request_with_retry(session, logger, f"{api_base}/tickets/past", params2, attempts, timeout)
    if r2.status_code == 200:
        try:
            data2 = r2.json()
        except Exception:
            return None, 200, "json_parse_error_past"
        if isinstance(data2, dict):
            return data2, 200, None
        if isinstance(data2, list) and data2:
            return data2[0], 200, None
        return None, 200, "empty_payload_past"

    try:
        return None, r2.status_code, r2.text
    except Exception:
        return None, r2.status_code, "http_error_past"


def reserve_next_missing(conn, days, cooldown_seconds):
    row = pg_one(
        conn,
        """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where last_seen >= now() - (%s * interval '1 day')
          and (last_attempt is null or last_attempt < now() - (%s * interval '1 second'))
        order by coalesce(last_attempt, 'epoch'::timestamptz) asc, ticket_id desc
        limit 1
        for update skip locked
        """,
        [int(days), int(cooldown_seconds)],
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


def update_missing_status(conn, ticket_id, status_code, err_text):
    pg_exec(
        conn,
        """
        update visualizacao_resolvidos.audit_recent_missing
        set last_status = %s,
            last_error = %s
        where ticket_id = %s
        """,
        [status_code, (err_text or "")[:4000] if err_text else None, int(ticket_id)],
    )


def delete_missing(conn, ticket_id):
    pg_exec(conn, "delete from visualizacao_resolvidos.audit_recent_missing where ticket_id=%s", [int(ticket_id)])


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
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
    api_base = os.getenv("MOVIDESK_API_BASE") or API_BASE_DEFAULT

    if not dsn or not token:
        raise SystemExit("NEON_DSN e MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN) são obrigatórios")

    days = env_int("DETAIL_MISSING_DAYS", 2)
    limit = env_int("DETAIL_MISSING_LIMIT", 50)
    cooldown_seconds = env_int("DETAIL_COOLDOWN_SECONDS", 60)
    api_attempts = env_int("DETAIL_API_ATTEMPTS", 4)
    connect_timeout = env_int("DETAIL_CONNECT_TIMEOUT", 10)
    read_timeout = env_int("DETAIL_READ_TIMEOUT", 120)
    timeout = (connect_timeout, read_timeout)

    session = requests.Session()

    updated = []
    failed = []

    processed = 0
    while processed < limit:
        conn = pg_connect(dsn)
        try:
            conn.autocommit = False
            ticket_id = reserve_next_missing(conn, days, cooldown_seconds)
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.info(f"db_reserve_fail: {type(e).__name__}: {e}")
            break
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if ticket_id is None:
            break

        ticket, sc, err = fetch_ticket_detail(session, logger, api_base, token, ticket_id, api_attempts, timeout)
        if ticket is None:
            conn2 = pg_connect(dsn)
            try:
                conn2.autocommit = False
                update_missing_status(conn2, ticket_id, sc, err or "ticket_none")
                conn2.commit()
            except Exception as e:
                try:
                    conn2.rollback()
                except Exception:
                    pass
                logger.info(f"db_update_missing_fail: {type(e).__name__}: {e}")
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
                update_missing_status(conn4, ticket_id, None, f"db_upsert_detail: {type(e).__name__}: {e}")
                conn4.commit()
            except Exception as e2:
                try:
                    conn4.rollback()
                except Exception:
                    pass
                logger.info(f"db_update_missing_fail: {type(e2).__name__}: {e2}")
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
