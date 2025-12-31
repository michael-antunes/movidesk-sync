import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql


def env_str(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def env_int(k: str, default: int) -> int:
    v = env_str(k)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def env_bool(k: str, default: bool = False) -> bool:
    v = env_str(k)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on", "sim")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    t = str(s).strip()
    if not t:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(t, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fmt_movidesk_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def pg_connect():
    dsn = env_str("DATABASE_URL") or env_str("NEON_DSN") or env_str("POSTGRES_URL") or env_str("PG_URL")
    if not dsn:
        raise RuntimeError("NEON_DSN/DATABASE_URL não definido")
    return psycopg2.connect(dsn)


def ensure_schema_and_table(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL(
                """
                create table if not exists {}.{} (
                  ticket_id bigint primary key,
                  merged_into_id bigint not null,
                  merged_at timestamptz,
                  raw_payload jsonb
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )
        cur.execute(sql.SQL("alter table {}.{} add column if not exists merged_into_id bigint").format(sql.Identifier(schema), sql.Identifier(table)))
        cur.execute(sql.SQL("alter table {}.{} add column if not exists merged_at timestamptz").format(sql.Identifier(schema), sql.Identifier(table)))
        cur.execute(sql.SQL("alter table {}.{} add column if not exists raw_payload jsonb").format(sql.Identifier(schema), sql.Identifier(table)))
    conn.commit()


def parse_ids_semicolon(v: Any) -> List[int]:
    if v is None:
        return []
    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(";") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


class MovideskClient:
    def __init__(self, base_url: str, token: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_merged_period(self, start_dt: datetime, end_dt: datetime, page: int) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/tickets/merged"
        params = {
            "token": self.token,
            "startDate": fmt_movidesk_dt(start_dt),
            "endDate": fmt_movidesk_dt(end_dt),
            "page": page,
        }
        for i in range(6):
            r = self.session.get(url, params=params, timeout=self.timeout)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 * (i + 1), 30))
                continue
            if r.status_code != 200:
                return None
            data = r.json()
            return data if isinstance(data, dict) else None
        return None

    @staticmethod
    def total_pages(page_number: Any) -> Optional[int]:
        if page_number is None:
            return None
        s = str(page_number)
        for sep in (" of ", " de "):
            if sep in s:
                try:
                    return int(s.split(sep)[1].strip())
                except Exception:
                    return None
        return None

    def iter_merged(self, start_dt: datetime, end_dt: datetime) -> Iterable[Dict[str, Any]]:
        page = 1
        total = None
        while True:
            data = self.get_merged_period(start_dt, end_dt, page)
            if not data:
                return
            items = data.get("mergedTickets") or data.get("value") or []
            if not items:
                return
            if total is None:
                total = self.total_pages(data.get("pageNumber"))
            for it in items:
                if isinstance(it, dict):
                    yield it
            if total is not None and page >= total:
                return
            page += 1
            time.sleep(0.15)


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Any]]) -> int:
    if not rows:
        return 0
    q = sql.SQL(
        """
        insert into {}.{} (ticket_id, merged_into_id, merged_at, raw_payload)
        values %s
        on conflict (ticket_id) do update set
          merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {}.{}.merged_at),
          raw_payload = excluded.raw_payload
        """
    ).format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(schema), sql.Identifier(table))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, q.as_string(conn), rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("tickets_mesclados")

    token = env_str("MOVIDESK_TOKEN") or env_str("MOVIDESK_API_TOKEN")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    base_url = env_str("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table = env_str("TABLE_NAME", "tickets_mesclados")

    lookback_days = env_int("LOOKBACK_DAYS", 30)
    window_days = env_int("WINDOW_DAYS", 7)
    dry_run = env_bool("DRY_RUN", False)

    now = utc_now()
    since_dt = now - timedelta(days=lookback_days)

    client = MovideskClient(base_url, token, timeout=60)

    conn = None
    if not dry_run:
        conn = pg_connect()
        conn.autocommit = False
        ensure_schema_and_table(conn, schema, table)

    total_rows = 0
    total_fetched = 0

    cur = since_dt
    step = timedelta(days=window_days if window_days > 0 else lookback_days)

    while cur < now:
        nxt = min(cur + step, now)
        batch: List[Tuple[int, int, Optional[datetime], Any]] = []

        for rec in client.iter_merged(cur, nxt):
            total_fetched += 1
            principal = rec.get("ticketId") or rec.get("id")
            if principal is None:
                continue
            try:
                principal_id = int(str(principal))
            except Exception:
                continue

            merged_ids = parse_ids_semicolon(rec.get("mergedTicketsIds") or rec.get("mergedTicketsIDs"))
            if not merged_ids:
                continue

            merged_at = parse_dt(rec.get("lastUpdate")) or parse_dt(rec.get("mergedDate")) or utc_now()
            payload = psycopg2.extras.Json(rec, dumps=lambda o: json.dumps(o, ensure_ascii=False))

            for mid in merged_ids:
                batch.append((int(mid), principal_id, merged_at, payload))

            if len(batch) >= 4000:
                if not dry_run and conn is not None:
                    total_rows += upsert_rows(conn, schema, table, batch)
                batch.clear()

        if batch:
            if not dry_run and conn is not None:
                total_rows += upsert_rows(conn, schema, table, batch)

        log.info("janela=%s..%s fetched=%s upsert_total=%s", cur.date().isoformat(), nxt.date().isoformat(), total_fetched, total_rows)
        cur = nxt

    if conn is not None:
        conn.close()


if __name__ == "__main__":
    main()
