import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
import psycopg2.extras


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("audit_recent_resolved_scan")


def utcnow():
    return datetime.now(timezone.utc)


def parse_iso_dt(s):
    if not s:
        return None
    if isinstance(s, datetime):
        dt = s
    elif isinstance(s, (int, float)):
        dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
    elif isinstance(s, str):
        s2 = s.strip()
        if not s2:
            return None
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s2)
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def env_int(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def movidesk_get_json(session, base_url, params, timeout):
    url = base_url.rstrip("/") + "/tickets"
    r = session.get(url, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"Movidesk returned non-JSON: {r.text[:500]}")


def normalize_items(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            return payload["items"]
        if isinstance(payload.get("value"), list):
            return payload["value"]
        if isinstance(payload.get("data"), list):
            return payload["data"]
        return []
    return []


def iter_tickets_by_lastupdate(session, base_url, token, start_dt, end_dt, page_size, timeout):
    skip = 0
    while True:
        flt = (
            f"lastUpdate ge {start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} and "
            f"lastUpdate le {end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} and "
            f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')"
        )
        params = {
            "token": token,
            "$select": "id,baseStatus,lastUpdate",
            "$filter": flt,
            "$orderby": "id",
            "$top": str(page_size),
            "$skip": str(skip),
            "includeDeletedItems": "true",
        }
        payload = movidesk_get_json(session, base_url, params, timeout=timeout)
        items = normalize_items(payload)
        if not items:
            return
        for row in items:
            if isinstance(row, dict):
                yield row.get("id"), row.get("lastUpdate")
        if len(items) < page_size:
            return
        skip += page_size


def pg_connect(dsn, application_name):
    try:
        return psycopg2.connect(dsn, application_name=application_name)
    except TypeError:
        return psycopg2.connect(dsn)


def ensure_unique_index(conn, schema):
    sql = f"create unique index if not exists audit_recent_missing_ticket_id_ux on {schema}.audit_recent_missing(ticket_id)"
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def get_last_window_end(conn, schema, notes_key, max_age_hours):
    sql = f"""
        select window_end
        from {schema}.audit_recent_run
        where notes = %s and window_end is not null
        order by id desc
        limit 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (notes_key,))
        row = cur.fetchone()
    if not row or not row[0]:
        return None
    dt = row[0]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if max_age_hours is not None and dt < utcnow() - timedelta(hours=max_age_hours):
        return None
    return dt


def create_run(conn, schema, window_start, window_end, meta):
    sql = f"""
        insert into {schema}.audit_recent_run
        (window_start, window_end, total_api, missing_total, total_local, run_at, window_from, window_to,
         workflow, job, ref, sha, repo, external_run_id, notes)
        values (%s,%s,0,0,0,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s)
        returning id, started_at
    """
    vals = (
        window_start,
        window_end,
        window_start,
        window_end,
        meta.get("workflow"),
        meta.get("job"),
        meta.get("ref"),
        meta.get("sha"),
        meta.get("repo"),
        meta.get("external_run_id"),
        meta.get("notes"),
    )
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        rid, started_at = cur.fetchone()
    conn.commit()
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    return rid, started_at.astimezone(timezone.utc)


def update_run(conn, schema, run_id, total_api, total_local, missing_total):
    sql = f"update {schema}.audit_recent_run set total_api=%s, total_local=%s, missing_total=%s where id=%s"
    with conn.cursor() as cur:
        cur.execute(sql, (total_api, total_local, missing_total, run_id))
    conn.commit()


def fetch_local_last_updates(conn, schema, table, ids):
    if not ids:
        return {}
    out = {}
    chunk = 5000
    sql = f"select ticket_id, last_update from {schema}.{table} where ticket_id = any(%s)"
    with conn.cursor() as cur:
        for i in range(0, len(ids), chunk):
            sub = ids[i:i + chunk]
            cur.execute(sql, (sub,))
            for tid, lu in cur.fetchall():
                if lu is None:
                    out[int(tid)] = None
                else:
                    if lu.tzinfo is None:
                        lu = lu.replace(tzinfo=timezone.utc)
                    out[int(tid)] = lu.astimezone(timezone.utc)
    return out


def count_local_in_window(conn, schema, table, start_dt, end_dt):
    sql = f"select count(*) from {schema}.{table} where last_update >= %s and last_update <= %s"
    with conn.cursor() as cur:
        cur.execute(sql, (start_dt, end_dt))
        return int(cur.fetchone()[0])


def upsert_missing(conn, schema, run_id, run_started_at, ticket_ids):
    if not ticket_ids:
        return 0
    now = utcnow()
    rows = [(run_id, int(tid), now, run_started_at) for tid in ticket_ids]
    sql = f"""
        insert into {schema}.audit_recent_missing (run_id, ticket_id, last_seen, run_started_at)
        values %s
        on conflict (ticket_id) do update
        set run_id = excluded.run_id,
            last_seen = excluded.last_seen,
            run_started_at = excluded.run_started_at
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(ticket_ids)


def main():
    neon_dsn = os.getenv("NEON_DSN") or os.getenv("DATABASE_URL")
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
    if not neon_dsn:
        logger.error("NEON_DSN/DATABASE_URL não definido")
        sys.exit(2)
    if not token:
        logger.error("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN não definido")
        sys.exit(2)

    base_url = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    schema = os.getenv("AUDIT_SCHEMA", "visualizacao_resolvidos")
    detail_table = os.getenv("DETAIL_TABLE", "tickets_resolvidos_detail")

    window_hours = env_int("AUDIT_WINDOW_HOURS", 48)
    if window_hours < 1:
        window_hours = 48

    overlap_minutes = env_int("AUDIT_OVERLAP_MINUTES", 10)
    if overlap_minutes < 0:
        overlap_minutes = 0

    state_max_age_hours = env_int("AUDIT_STATE_MAX_AGE_HOURS", 24 * 14)

    timeout = env_int("MOVIDESK_TIMEOUT", 30)
    page_size = env_int("MOVIDESK_PAGE_SIZE", 1000)
    if page_size <= 0 or page_size > 5000:
        page_size = 1000

    notes_key = os.getenv("AUDIT_NOTES_KEY", "scan lastUpdate")

    meta = {
        "workflow": os.getenv("GITHUB_WORKFLOW"),
        "job": os.getenv("GITHUB_JOB"),
        "ref": os.getenv("GITHUB_REF_NAME") or os.getenv("GITHUB_REF"),
        "sha": os.getenv("GITHUB_SHA"),
        "repo": os.getenv("GITHUB_REPOSITORY"),
        "external_run_id": os.getenv("GITHUB_RUN_ID"),
        "notes": notes_key,
    }

    now = utcnow()
    base_start = now - timedelta(hours=window_hours)

    with pg_connect(neon_dsn, application_name="movidesk-audit-recent") as conn:
        conn.autocommit = False
        ensure_unique_index(conn, schema)

        last_end = get_last_window_end(conn, schema, notes_key, state_max_age_hours)
        if last_end is not None:
            win_start = last_end - timedelta(minutes=overlap_minutes)
            if win_start < base_start:
                win_start = base_start
        else:
            win_start = base_start

        win_end = now
        if win_start >= win_end:
            win_start = win_end - timedelta(minutes=max(1, overlap_minutes or 1))

        run_id, run_started_at = create_run(conn, schema, win_start, win_end, meta)

        sess = requests.Session()
        api_ids = []
        api_last_update = {}
        try:
            for tid, lu in iter_tickets_by_lastupdate(sess, base_url, token, win_start, win_end, page_size, timeout):
                try:
                    tid_i = int(tid)
                except Exception:
                    continue
                dt = parse_iso_dt(lu)
                if dt is None:
                    continue
                api_ids.append(tid_i)
                api_last_update[tid_i] = dt
        finally:
            sess.close()

        if not api_ids:
            total_local = count_local_in_window(conn, schema, detail_table, win_start, win_end)
            update_run(conn, schema, run_id, 0, total_local, 0)
            logger.info(f"window={win_start.isoformat()}..{win_end.isoformat()} api=0 local={total_local} missing=0")
            return

        api_ids = sorted(set(api_ids))
        local_map = fetch_local_last_updates(conn, schema, detail_table, api_ids)

        stale = []
        for tid in api_ids:
            api_dt = api_last_update.get(tid)
            db_dt = local_map.get(tid)
            if db_dt is None:
                stale.append(tid)
            elif api_dt is not None and api_dt > db_dt + timedelta(seconds=1):
                stale.append(tid)

        missing_upserted = upsert_missing(conn, schema, run_id, run_started_at, stale)
        total_local = count_local_in_window(conn, schema, detail_table, win_start, win_end)
        update_run(conn, schema, run_id, len(api_ids), total_local, missing_upserted)

        sample = ",".join(str(x) for x in stale[:20])
        logger.info(
            f"window={win_start.isoformat()}..{win_end.isoformat()} api={len(api_ids)} local={total_local} missing={missing_upserted}"
            + (f" sample_missing=[{sample}]" if sample else "")
        )


if __name__ == "__main__":
    main()
