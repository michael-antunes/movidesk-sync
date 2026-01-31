import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_loop_latest50_2026-01-31"


def env_str(k: str, default: Optional[str] = None) -> str:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {k}")
        return default
    return v.strip()


def env_int(k: str, default: int) -> int:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def env_float(k: str, default: float) -> float:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v.strip())
    except Exception:
        return default


def qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def to_int(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except Exception:
        try:
            s = str(v).strip()
            return int(s) if s else None
        except Exception:
            return None


def parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                d = datetime.strptime(s, fmt)
                return d.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def req(sess: requests.Session, url: str, params: Dict[str, Any], timeout: int, attempts: int) -> Tuple[Optional[Dict[str, Any]], int]:
    last_status = 0
    for i in range(attempts):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 200:
                try:
                    return r.json(), 200
                except Exception:
                    return None, 200
            if r.status_code == 404:
                return None, 404
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("retry-after")
                if ra:
                    try:
                        time.sleep(max(1, int(float(ra))))
                    except Exception:
                        time.sleep(min(2 ** i, 30))
                else:
                    time.sleep(min(2 ** i, 30))
                continue
            return None, r.status_code
        except Exception:
            time.sleep(min(2 ** i, 30))
    return None, last_status


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def try_lock(conn, key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select pg_try_advisory_lock(hashtext(%s))", (key,))
        return bool(cur.fetchone()[0])


def unlock(conn, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_unlock(hashtext(%s))", (key,))


def ensure_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {qident(schema)}")
        cur.execute(
            f"""
            create table if not exists {qname(schema, table)} (
              ticket_id integer primary key,
              merged_into_id integer not null,
              merged_at timestamptz,
              raw_payload jsonb
            )
            """
        )
        cur.execute(f"create index if not exists ix_{table}_merged_into_id on {qname(schema, table)} (merged_into_id)")
        cur.execute(f"create index if not exists ix_{table}_merged_at on {qname(schema, table)} (merged_at)")


def upsert_rows(
    conn,
    schema: str,
    table: str,
    rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]],
) -> int:
    if not rows:
        return 0
    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for (child_id, parent_id, merged_at, payload) in rows:
        if child_id is None or parent_id is None:
            continue
        cid = int(child_id)
        prev = dedup.get(cid)
        if prev is None:
            dedup[cid] = (cid, int(parent_id), merged_at, payload)
        else:
            prev_at = prev[2]
            if prev_at is None and merged_at is not None:
                dedup[cid] = (cid, int(parent_id), merged_at, payload)
            elif prev_at is not None and merged_at is not None and merged_at > prev_at:
                dedup[cid] = (cid, int(parent_id), merged_at, payload)

    rows2 = list(dedup.values())
    if not rows2:
        return 0

    sql = f"""
    insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update
      set merged_into_id = excluded.merged_into_id,
          merged_at = excluded.merged_at,
          raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows2, page_size=1000)
    return len(rows2)


def cleanup_other_tables(
    conn,
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    excluidos_schema: str,
    excluidos_table: str,
    merged_rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]],
) -> Tuple[int, int, int]:
    if not merged_rows:
        return 0, 0, 0

    tmp_rows = []
    for (child_id, _parent_id, merged_at, _payload) in merged_rows:
        if child_id is None:
            continue
        tmp_rows.append((int(child_id), merged_at))

    if not tmp_rows:
        return 0, 0, 0

    with conn.cursor() as cur:
        cur.execute("create temporary table tmp_merged_children(ticket_id bigint primary key, merged_at timestamptz) on commit drop")
        psycopg2.extras.execute_values(cur, "insert into tmp_merged_children(ticket_id, merged_at) values %s", tmp_rows, page_size=1000)

        cur.execute(
            f"""
            delete from {qname(resolvidos_schema, resolvidos_table)} tr
            using tmp_merged_children t
            where tr.ticket_id = t.ticket_id
              and (t.merged_at is null or coalesce(tr.last_update, tr.updated_at, 'epoch'::timestamptz) <= t.merged_at)
            """
        )
        d_res = cur.rowcount

        cur.execute(
            f"""
            delete from {qname(abertos_schema, abertos_table)} ta
            using tmp_merged_children t
            where ta.ticket_id = t.ticket_id
              and (t.merged_at is null or coalesce(ta.last_update, ta.updated_at, 'epoch'::timestamptz) <= t.merged_at)
            """
        )
        d_abe = cur.rowcount

        cur.execute(
            f"""
            delete from {qname(excluidos_schema, excluidos_table)} te
            using tmp_merged_children t
            where te.ticket_id = t.ticket_id
              and (
                    t.merged_at is null
                    or coalesce(
                        te.last_update,
                        nullif(te.raw->>'lastUpdate','')::timestamptz,
                        te.date_excluido,
                        te.synced_at,
                        'epoch'::timestamptz
                      ) <= t.merged_at
                  )
            """
        )
        d_exc = cur.rowcount

    return d_res, d_abe, d_exc


def fetch_latest_merged(
    sess: requests.Session,
    base_url: str,
    token: str,
    start_date: str,
    end_date: Optional[str],
    timeout: int,
    attempts: int,
) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params: Dict[str, Any] = {"token": token, "startDate": start_date, "page": "1"}
    if end_date:
        params["endDate"] = end_date
    data, st = req(sess, url, params, timeout, attempts)
    if st != 200 or not isinstance(data, dict):
        return {}
    return data


def main():
    logging.basicConfig(level=getattr(logging, env_str("LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    merged_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    merged_table = env_str("TABLE_NAME", "tickets_mesclados")

    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos")
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail")

    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos")

    excluidos_schema = env_str("EXCLUIDOS_SCHEMA", "visualizacao_resolvidos")
    excluidos_table = env_str("EXCLUIDOS_TABLE", "tickets_excluidos")

    window_size = env_int("WINDOW_SIZE", 50)
    http_timeout = env_int("HTTP_TIMEOUT", 45)
    attempts = env_int("HTTP_ATTEMPTS", 6)

    loop_sleep_seconds = env_float("LOOP_SLEEP_SECONDS", 60.0)
    loop_max_cycles = env_int("LOOP_MAX_CYCLES", 0)

    start_date = os.getenv("MERGED_START_DATE")
    if not start_date or start_date.strip() == "":
        start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = os.getenv("MERGED_END_DATE")
    if end_date and end_date.strip() == "":
        end_date = None

    log.info(
        "script_version=%s start_date=%s end_date=%s window_size=%d loop_sleep_seconds=%.2f loop_max_cycles=%d",
        SCRIPT_VERSION,
        start_date,
        end_date or "",
        window_size,
        loop_sleep_seconds,
        loop_max_cycles,
    )

    sess = requests.Session()

    conn = pg_connect(dsn)
    conn.autocommit = False
    try:
        if not try_lock(conn, "sync_tickets_merged_loop"):
            log.info("another_run_detected exiting")
            conn.rollback()
            return

        ensure_table(conn, merged_schema, merged_table)
        conn.commit()

        cycles = 0
        while True:
            cycles += 1

            data = fetch_latest_merged(sess, base_url, token, start_date, end_date, http_timeout, attempts)

            merged_list = data.get("mergedTickets")
            if not isinstance(merged_list, list):
                merged_list = []
            merged_list = merged_list[: max(0, int(window_size))]

            rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []
            for item in merged_list:
                if not isinstance(item, dict):
                    continue
                parent_id = to_int(item.get("ticketId"))
                if not parent_id:
                    continue
                merged_at = parse_dt(item.get("lastUpdate")) or datetime.now(timezone.utc)
                ids_str = str(item.get("mergedTicketsIds") or "").strip()
                if not ids_str:
                    continue
                child_ids = []
                for s in ids_str.split(";"):
                    v = to_int(s)
                    if v:
                        child_ids.append(v)
                if not child_ids:
                    continue
                raw = {"parentTicketId": str(parent_id), **item}
                payload = json_payload(raw)
                for child_id in child_ids:
                    rows.append((int(child_id), int(parent_id), merged_at, payload))

            upserted = 0
            d_res = 0
            d_abe = 0
            d_exc = 0

            if rows:
                upserted = upsert_rows(conn, merged_schema, merged_table, rows)
                d_res, d_abe, d_exc = cleanup_other_tables(
                    conn,
                    resolvidos_schema,
                    resolvidos_table,
                    abertos_schema,
                    abertos_table,
                    excluidos_schema,
                    excluidos_table,
                    rows,
                )
                conn.commit()
            else:
                conn.commit()

            log.info(
                "cycle=%d parents=%d upserted_children=%d removed_resolvidos=%d removed_abertos=%d removed_excluidos=%d",
                cycles,
                len(merged_list),
                upserted,
                d_res,
                d_abe,
                d_exc,
            )

            if loop_max_cycles and cycles >= loop_max_cycles:
                break

            time.sleep(max(0.0, float(loop_sleep_seconds)))

    finally:
        try:
            unlock(conn, "sync_tickets_merged_loop")
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
