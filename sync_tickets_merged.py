import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


def env_str(k: str, default: Optional[str] = None) -> str:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env {k}")
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


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def parse_dt(v: Any) -> Optional[dt.datetime]:
    if not v:
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        x = dt.datetime.fromisoformat(s)
        return x if x.tzinfo else x.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, list):
        out = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    s = str(v).strip()
    if not s:
        return []
    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def ensure_table(dsn: str, schema: str, table: str):
    conn = pg_connect(dsn)
    conn.autocommit = False
    try:
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
            cur.execute(f"create index if not exists ix_tickets_mesclados_merged_into on {qname(schema, table)} (merged_into_id)")
        conn.commit()
    finally:
        conn.close()


def fetch_candidate_ids(dsn: str, src_schema: str, src_table: str, src_date_col: str, since_utc: dt.datetime, max_ids: int) -> List[int]:
    conn = pg_connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select ticket_id
                from (
                  select ticket_id::bigint as ticket_id
                  from {qname(src_schema, src_table)}
                  where {qident(src_date_col)} >= %s
                  group by ticket_id
                ) t
                order by ticket_id desc
                limit %s
                """,
                (since_utc, max_ids),
            )
            return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def upsert_rows(dsn: str, schema: str, table: str, rows: List[Tuple[int, int, Optional[dt.datetime], Any]]) -> int:
    if not rows:
        return 0
    sql = f"""
        insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
        values %s
        on conflict (ticket_id) do update set
          merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {qname(schema, table)}.merged_at),
          raw_payload = excluded.raw_payload
    """
    for attempt in range(6):
        conn = None
        try:
            conn = pg_connect(dsn)
            conn.autocommit = False
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
            conn.commit()
            conn.close()
            return len(rows)
        except psycopg2.OperationalError:
            try:
                if conn is not None and getattr(conn, "closed", 1) == 0:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            time.sleep(2 * (attempt + 1))
            continue
        except Exception:
            try:
                if conn is not None and getattr(conn, "closed", 1) == 0:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            raise
    raise RuntimeError("Falha ao gravar no Neon após retries")


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    for key in ("ticketId", "id"):
        params = {"token": token, key: str(ticket_id)}
        last_status = None
        for i in range(5):
            try:
                r = sess.get(url, params=params, timeout=timeout)
                last_status = r.status_code
                if r.status_code == 404:
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 * (i + 1))
                    continue
                if r.status_code != 200:
                    return None, r.status_code
                data = r.json()
                return (data if isinstance(data, dict) else None), 200
            except Exception:
                time.sleep(2 * (i + 1))
        if last_status == 404:
            continue
    return None, 404


def extract_rows(queried_id: int, raw: Dict[str, Any]) -> List[Tuple[int, int, Optional[dt.datetime], Any]]:
    principal_id = raw.get("ticketId") or raw.get("id") or raw.get("ticket_id")
    try:
        principal_id_int = int(principal_id) if principal_id is not None else int(queried_id)
    except Exception:
        principal_id_int = int(queried_id)

    merged_at = parse_dt(raw.get("mergedDate") or raw.get("mergedAt") or raw.get("performedAt") or raw.get("date") or raw.get("lastUpdate") or raw.get("last_update"))
    payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))

    merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
    if not merged_ids:
        mt = raw.get("mergedTickets") or raw.get("mergedTicketsList")
        if isinstance(mt, list):
            tmp = []
            for it in mt:
                if isinstance(it, dict):
                    mid = it.get("id") or it.get("ticketId") or it.get("ticket_id")
                    if mid is not None:
                        try:
                            tmp.append(int(mid))
                        except Exception:
                            pass
            merged_ids = tmp

    rows: List[Tuple[int, int, Optional[dt.datetime], Any]] = []
    if merged_ids:
        for mid in merged_ids:
            try:
                rows.append((int(mid), principal_id_int, merged_at, payload))
            except Exception:
                pass
        return rows

    merged_into = raw.get("mergedIntoId") or raw.get("mergedIntoTicketId") or raw.get("mainTicketId") or raw.get("principalTicketId") or raw.get("principalId") or raw.get("mergedInto")
    if merged_into is not None:
        try:
            rows.append((int(queried_id), int(merged_into), merged_at, payload))
        except Exception:
            pass
    return rows


def pick_row(old: Optional[Tuple[int, int, Optional[dt.datetime], Any]], new: Tuple[int, int, Optional[dt.datetime], Any]):
    if old is None:
        return new
    old_at = old[2]
    new_at = new[2]
    if old_at is None and new_at is not None:
        return new
    if old_at is not None and new_at is not None and new_at > old_at:
        return new
    return old


def main():
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("merged-by-ids")

    token = env_str("MOVIDESK_TOKEN")
    base_url = env_str("MOVIDESK_BASE_URL", env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1"))
    neon_dsn = env_str("NEON_DSN")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table = env_str("TABLE_NAME", "tickets_mesclados")

    src_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    src_table = env_str("SOURCE_TABLE", "tickets_suporte")
    src_date_col = env_str("SOURCE_DATE_COL", "updated_at")

    lookback_days = env_int("LOOKBACK_DAYS", 2)
    max_ids = env_int("MAX_IDS", 20000)

    rpm = env_float("RPM", 9.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    http_timeout = env_int("HTTP_TIMEOUT", 60)

    flush_unique = env_int("FLUSH_UNIQUE", 500)
    log_every = env_int("LOG_EVERY", 50)

    since_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=lookback_days)

    ensure_table(neon_dsn, schema, table)

    ids = fetch_candidate_ids(neon_dsn, src_schema, src_table, src_date_col, since_utc, max_ids)
    if not ids:
        log.info("scan_ids_begin empty since_utc=%s", since_utc.isoformat())
        return

    start_id = ids[0]
    end_id = ids[-1]

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    checked = 0
    found = 0
    upserted_total = 0
    status_counts: Dict[int, int] = {}

    first_checked = None
    last_checked = None

    rows_map: Dict[int, Tuple[int, int, Optional[dt.datetime], Any]] = {}

    log.info("scan_ids_begin start_id=%s end_id=%s count=%s since_utc=%s", start_id, end_id, len(ids), since_utc.isoformat())

    for ticket_id in ids:
        if first_checked is None:
            first_checked = int(ticket_id)
        last_checked = int(ticket_id)

        checked += 1
        raw, st = movidesk_get_merged(sess, base_url, token, int(ticket_id), http_timeout)
        if st is not None:
            status_counts[st] = status_counts.get(st, 0) + 1

        if raw:
            rows = extract_rows(int(ticket_id), raw)
            if rows:
                found += len(rows)
                for r in rows:
                    rows_map[r[0]] = pick_row(rows_map.get(r[0]), r)

        if throttle > 0:
            time.sleep(throttle)

        if len(rows_map) >= flush_unique:
            upserted_total += upsert_rows(neon_dsn, schema, table, list(rows_map.values()))
            rows_map.clear()

        if checked % log_every == 0:
            log.info("progress checked=%s found=%s upserted=%s unique_buf=%s last_id=%s", checked, found, upserted_total, len(rows_map), ticket_id)

    if rows_map:
        upserted_total += upsert_rows(neon_dsn, schema, table, list(rows_map.values()))
        rows_map.clear()

    log.info(
        "scan_ids_end start_id=%s end_id=%s first_checked=%s last_checked=%s checked=%s found=%s upserted=%s statuses=%s",
        start_id,
        end_id,
        first_checked,
        last_checked,
        checked,
        found,
        upserted_total,
        json.dumps(status_counts, ensure_ascii=False),
    )


if __name__ == "__main__":
    main()
