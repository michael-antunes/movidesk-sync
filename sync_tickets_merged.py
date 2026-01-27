import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Iterable

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
    if isinstance(v, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(v), tz=dt.timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return dt.datetime.fromisoformat(s)
        except Exception:
            return None
    return None


def to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[int] = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",")]
        out: List[int] = []
        for p in parts:
            if not p:
                continue
            try:
                out.append(int(p))
            except Exception:
                pass
        return out
    try:
        return [int(v)]
    except Exception:
        return []


def pick_row(old: Optional[Tuple[int, int, Optional[dt.datetime], Any]],
             new: Tuple[int, int, Optional[dt.datetime], Any]) -> Tuple[int, int, Optional[dt.datetime], Any]:
    if old is None:
        return new
    ticket_id, merged_into_id, merged_at, payload = new
    _, old_merged_into_id, old_merged_at, _ = old
    final_merged_at = merged_at or old_merged_at
    return (ticket_id, merged_into_id, final_merged_at, payload)


class NeonDB:
    def __init__(self, dsn: str, log: logging.Logger):
        self.dsn = dsn
        self.log = log
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._last_ping = 0.0

    def connect(self) -> None:
        if self.conn is not None:
            return
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = False

    def close(self) -> None:
        try:
            if self.conn is not None:
                self.conn.close()
        finally:
            self.conn = None

    def ping_if_needed(self, every_seconds: int) -> None:
        if every_seconds <= 0:
            return
        now = time.time()
        if now - self._last_ping < every_seconds:
            return
        self._last_ping = now
        try:
            self.connect()
            with self.conn.cursor() as cur:
                cur.execute("select 1")
            self.conn.commit()
        except Exception:
            try:
                if self.conn is not None:
                    self.conn.rollback()
            except Exception:
                pass
            self.close()

    def ensure_table(self, schema: str, table: str) -> None:
        self.connect()
        with self.conn.cursor() as cur:
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
        self.conn.commit()

    def fetch_candidate_ids(
        self,
        src_schema: str,
        src_table: str,
        src_date_col: str,
        since_utc: dt.datetime,
        max_ids: int
    ) -> List[int]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                select ticket_id
                from (
                  select ticket_id::bigint as ticket_id,
                         max({qident(src_date_col)}) as last_upd
                  from {qname(src_schema, src_table)}
                  where {qident(src_date_col)} >= %s
                  group by ticket_id
                ) t
                order by last_upd desc, ticket_id desc
                limit %s
                """,
                (since_utc, max_ids),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
        self.conn.commit()
        return ids

    def upsert_rows(self, schema: str, table: str, rows: List[Tuple[int, int, Optional[dt.datetime], Any]]) -> int:
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
        last_err = None
        for attempt in range(8):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
                self.conn.commit()
                return len(rows)
            except Exception as e:
                last_err = repr(e)
                try:
                    if self.conn is not None:
                        self.conn.rollback()
                except Exception:
                    pass
                self.close()
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"Falha ao gravar no Neon após retries: {last_err}")


def parse_query_keys(raw: str) -> List[str]:
    keys = []
    for part in (raw or "").split(","):
        k = part.strip()
        if k:
            keys.append(k)
    return keys or ["ticketId"]


def movidesk_get_merged(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
    throttle_seconds: float,
    query_keys: List[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"

    for key in query_keys:
        params = {"token": token, key: str(ticket_id)}
        last_status = None

        for i in range(5):
            try:
                r = sess.get(url, params=params, timeout=timeout)
                last_status = r.status_code

                if throttle_seconds > 0:
                    time.sleep(throttle_seconds)

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
                if throttle_seconds > 0:
                    time.sleep(throttle_seconds)
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

    merged_at = parse_dt(
        raw.get("mergedDate")
        or raw.get("mergedAt")
        or raw.get("performedAt")
        or raw.get("date")
        or raw.get("lastUpdate")
        or raw.get("last_update")
    )
    payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))

    merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketIds") or raw.get("mergedTickets") or raw.get("merged_tickets"))
    if not merged_ids:
        merged_into = raw.get("mergedIntoId") or raw.get("merged_into_id") or raw.get("mergedInto") or raw.get("mergedIntoTicketId")
        try:
            merged_into_int = int(merged_into) if merged_into is not None else principal_id_int
        except Exception:
            merged_into_int = principal_id_int
        return [(int(queried_id), int(merged_into_int), merged_at, payload)]

    out: List[Tuple[int, int, Optional[dt.datetime], Any]] = []
    for mid in merged_ids:
        out.append((int(mid), int(principal_id_int), merged_at, payload))
    return out


def setup_logger() -> logging.Logger:
    lvl = env_str("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sync_tickets_merged")


def main() -> None:
    log = setup_logger()

    token = env_str("MOVIDESK_TOKEN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    neon_dsn = env_str("NEON_DSN")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table = env_str("TABLE_NAME", "tickets_mesclados")

    src_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    src_table = env_str("SOURCE_TABLE", "tickets_suporte")
    src_date_col = env_str("SOURCE_DATE_COL", "updated_at")

    lookback_hours = env_int("LOOKBACK_HOURS", 2)
    max_ids = env_int("MAX_IDS", 50)

    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    http_timeout = env_int("HTTP_TIMEOUT", 45)

    query_keys = parse_query_keys(env_str("MERGED_QUERY_KEYS", "ticketId"))

    flush_unique = env_int("FLUSH_UNIQUE", 200)
    log_every = env_int("LOG_EVERY", 5)
    ping_every = env_int("DB_PING_EVERY", 30)

    since_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)

    db = NeonDB(neon_dsn, log)
    db.ensure_table(schema, table)

    ids = db.fetch_candidate_ids(src_schema, src_table, src_date_col, since_utc, max_ids)
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

    log.info("scan_ids_begin start_id=%s end_id=%s count=%s since_utc=%s max_ids=%s query_keys=%s",
             start_id, end_id, len(ids), since_utc.isoformat(), max_ids, ",".join(query_keys))

    for ticket_id in ids:
        if first_checked is None:
            first_checked = int(ticket_id)
        last_checked = int(ticket_id)

        checked += 1

        raw, st = movidesk_get_merged(
            sess=sess,
            base_url=base_url,
            token=token,
            ticket_id=int(ticket_id),
            timeout=http_timeout,
            throttle_seconds=throttle,
            query_keys=query_keys,
        )
        if st is not None:
            status_counts[st] = status_counts.get(st, 0) + 1

        if raw:
            rows = extract_rows(int(ticket_id), raw)
            if rows:
                found += len(rows)
                for r in rows:
                    rows_map[r[0]] = pick_row(rows_map.get(r[0]), r)

        db.ping_if_needed(ping_every)

        if len(rows_map) >= flush_unique:
            upserted_total += db.upsert_rows(schema, table, list(rows_map.values()))
            rows_map.clear()

        if checked % log_every == 0:
            log.info("progress checked=%s found=%s upserted=%s unique_buf=%s last_id=%s", checked, found, upserted_total, len(rows_map), ticket_id)

    if rows_map:
        upserted_total += db.upsert_rows(schema, table, list(rows_map.values()))
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
