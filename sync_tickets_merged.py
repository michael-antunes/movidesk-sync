import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_by_date_page_into_children_2026-01-31"


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


def qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def parse_merged_lastupdate(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


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


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def ensure_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {qident(schema)}")
        cur.execute(
            f"""
            create table if not exists {qname(schema, table)} (
              ticket_id bigint primary key,
              merged_into_id bigint not null,
              merged_at timestamptz,
              raw_payload jsonb
            )
            """
        )
        cur.execute(f"create index if not exists ix_{table}_merged_into_id on {qname(schema, table)} (merged_into_id)")
        cur.execute(f"create index if not exists ix_{table}_merged_at on {qname(schema, table)} (merged_at)")


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]) -> int:
    if not rows:
        return 0
    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for (child_id, parent_id, merged_at, payload) in rows:
        if child_id is None or parent_id is None:
            continue
        dedup[int(child_id)] = (int(child_id), int(parent_id), merged_at, payload)
    rows2 = list(dedup.values())
    if not rows2:
        return 0
    sql = f"""
    insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update
      set merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {qname(schema, table)}.merged_at),
          raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows2, page_size=1000)
    return len(rows2)


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


def read_range_control_page(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select id_atual_merged from {qname(schema, table)} limit 1")
        row = cur.fetchone()
    if not row or row[0] is None:
        return 1
    v = to_int(row[0])
    return v if v and v > 0 else 1


def write_range_control_page(conn, schema: str, table: str, page: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f"update {qname(schema, table)} set id_atual_merged=%s", (int(page),))
    conn.commit()


def main():
    logging.basicConfig(level=getattr(logging, env_str("LOG_LEVEL", "INFO").upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    db_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table_name = env_str("TABLE_NAME", "tickets_mesclados")

    control_schema = env_str("CONTROL_SCHEMA", "visualizacao_resolvidos")
    control_table = env_str("CONTROL_TABLE", "range_scan_control")

    window_size = env_int("WINDOW_SIZE", 50)
    http_timeout = env_int("HTTP_TIMEOUT", 45)
    attempts = env_int("HTTP_ATTEMPTS", 6)

    start_date = os.getenv("MERGED_START_DATE")
    if not start_date or start_date.strip() == "":
        start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = os.getenv("MERGED_END_DATE")
    if end_date and end_date.strip() == "":
        end_date = None

    log.info("script_version=%s start_date=%s end_date=%s window_size=%d", SCRIPT_VERSION, start_date, end_date or "", window_size)

    conn = pg_connect(dsn)
    conn.autocommit = False
    try:
        ensure_table(conn, db_schema, table_name)
        conn.commit()
        current_page = read_range_control_page(conn, control_schema, control_table)
    finally:
        conn.close()

    sess = requests.Session()
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "startDate": start_date, "page": str(current_page)}
    if end_date:
        params["endDate"] = end_date

    data, st = req(sess, url, params, http_timeout, attempts)
    if st != 200 or not isinstance(data, dict):
        log.info("merged_page page=%s status=%s rows=0", current_page, st)
        return

    page_number = str(data.get("pageNumber") or "").strip()
    total_pages = None
    if "of" in page_number:
        parts = [p.strip() for p in page_number.split("of", 1)]
        if len(parts) == 2:
            total_pages = to_int(parts[1])
    if not total_pages or total_pages <= 0:
        total_pages = current_page

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
        merged_at = parse_merged_lastupdate(item.get("lastUpdate")) or datetime.now(timezone.utc)
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

    conn2 = pg_connect(dsn)
    conn2.autocommit = False
    try:
        n = upsert_rows(conn2, db_schema, table_name, rows)
        next_page = current_page + 1
        if next_page > int(total_pages):
            next_page = 1
        write_range_control_page(conn2, control_schema, control_table, next_page)
        conn2.commit()
        log.info("merged_page page=%s/%s parents=%s inserted_or_updated=%s next_page=%s", current_page, total_pages, len(merged_list), n, next_page)
    finally:
        try:
            conn2.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
