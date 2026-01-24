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


def ensure_table(conn, schema: str, table: str):
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


def fetch_candidate_ids(conn, src_schema: str, src_table: str, src_date_col: str, since_utc: dt.datetime, max_ids: int) -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select ticket_id::bigint
            from {qname(src_schema, src_table)}
            where {qident(src_date_col)} >= %s
            order by ticket_id desc
            limit %s
            """,
            (since_utc, max_ids),
        )
        return [int(r[0]) for r in cur.fetchall()]


def movidesk_get_merged_by_ticket(sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
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


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[dt.datetime], Any]]):
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
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


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
    commit_every = env_int("COMMIT_EVERY", 50)

    since_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=lookback_days)

    conn = psycopg2.connect(neon_dsn)
    conn.autocommit = False
    try:
        ensure_table(conn, schema, table)
        conn.commit()

        ids = fetch_candidate_ids(conn, src_schema, src_table, src_date_col, since_utc, max_ids)
        if not ids:
            log.info("scan_ids_begin empty")
            return

        start_id = ids[0]
        end_id = ids[-1]
        log.info("scan_ids_begin start_id=%s end_id=%s count=%s since_utc=%s", start_id, end_id, len(ids), since_utc.isoformat())

        sess = requests.Session()
        sess.headers.update({"Accept": "application/json"})

        checked = 0
        found = 0
        upserted_total = 0
        rows_buf: List[Tuple[int, int, Optional[dt.datetime], Any]] = []
        status_counts: Dict[int, int] = {}

        for ticket_id in ids:
            checked += 1
            raw, st = movidesk_get_merged_by_ticket(sess, base_url, token, int(ticket_id), http_timeout)
            if st is not None:
                status_counts[st] = status_counts.get(st, 0) + 1
            if raw:
                rows = extract_rows(int(ticket_id), raw)
                if rows:
                    found += len(rows)
                    rows_buf.extend(rows)

            if throttle > 0:
                time.sleep(throttle)

            if len(rows_buf) >= 2000 or checked % commit_every == 0:
                if rows_buf:
                    upserted_total += upsert_rows(conn, schema, table, rows_buf)
                    rows_buf.clear()
                conn.commit()
                log.info("progress checked=%s found=%s upserted=%s last_id=%s", checked, found, upserted_total, ticket_id)

        if rows_buf:
            upserted_total += upsert_rows(conn, schema, table, rows_buf)
            rows_buf.clear()
        conn.commit()

        log.info("scan_ids_end start_id=%s end_id=%s checked=%s found=%s upserted=%s statuses=%s", start_id, end_id, checked, found, upserted_total, json.dumps(status_counts, ensure_ascii=False))

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
