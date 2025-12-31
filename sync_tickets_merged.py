import os
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


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
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    return None


def fmt_movidesk_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class SyncCfg:
    movidesk_token: str
    base_url: str
    db_schema: str
    table_name: str
    lookback_days: int
    window_days: int
    dry_run: bool


def pg_connect() -> psycopg2.extensions.connection:
    dsn = env_str("DATABASE_URL") or env_str("NEON_DSN") or env_str("POSTGRES_URL") or env_str("PG_URL")
    if dsn:
        return psycopg2.connect(dsn)
    host = env_str("PGHOST")
    db = env_str("PGDATABASE")
    user = env_str("PGUSER")
    pwd = env_str("PGPASSWORD")
    port = env_str("PGPORT", "5432")
    if not all([host, db, user, pwd]):
        raise RuntimeError("Credenciais do Postgres ausentes")
    return psycopg2.connect(host=host, dbname=db, user=user, password=pwd, port=port)


def qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def ensure_table(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'create schema if not exists {qident(schema)}')
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
        cur.execute(
            f"""
            do $$
            begin
              if exists (
                select 1
                from information_schema.columns
                where table_schema='{schema}'
                  and table_name='{table}'
                  and column_name='raw_paylcad'
              ) then
                execute 'alter table {qname(schema, table)} rename column raw_paylcad to raw_payload';
              end if;
            end $$;
            """
        )
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_into_id bigint")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_at timestamptz")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists raw_payload jsonb")
        cur.execute(f"delete from {qname(schema, table)} where merged_into_id is null")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists situacao_mesclado")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_tickets")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_tickets_ids")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_ticket_ids_arr")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists last_update")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists synced_at")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists updated_at")
        cur.execute(
            f"""
            do $$
            begin
              if not exists (
                select 1
                from pg_constraint
                where conrelid='{schema}.{table}'::regclass
                  and contype='p'
              ) then
                execute 'alter table {qname(schema, table)} add primary key (ticket_id)';
              end if;
            end $$;
            """
        )
    conn.commit()


def parse_page_total(page_number: Any) -> Optional[int]:
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


def parse_ids_semicolon(v: Any) -> List[int]:
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
        last_err = None
        for i in range(6):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 * (i + 1), 30))
                    continue
                if r.status_code != 200:
                    last_err = f"{r.status_code} {r.text}"
                    break
                data = r.json()
                return data if isinstance(data, dict) else None
            except Exception as e:
                last_err = str(e)
                time.sleep(min(2 * (i + 1), 30))
        raise RuntimeError(last_err or "Falha Movidesk")

    def iter_merged(self, start_dt: datetime, end_dt: datetime) -> Iterable[Dict[str, Any]]:
        page = 1
        total = None
        while True:
            data = self.get_merged_period(start_dt, end_dt, page)
            items = data.get("mergedTickets") or []
            if not items:
                return
            if total is None:
                total = parse_page_total(data.get("pageNumber"))
            for it in items:
                if isinstance(it, dict):
                    yield it
            if total is not None and page >= total:
                return
            page += 1
            time.sleep(0.15)


def upsert_rows(conn: psycopg2.extensions.connection, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Any]]) -> int:
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
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync-merged")

    cfg = SyncCfg(
        movidesk_token=env_str("MOVIDESK_TOKEN") or env_str("MOVIDESK_API_TOKEN") or "",
        base_url=env_str("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1"),
        db_schema=env_str("DB_SCHEMA", "visualizacao_resolvidos"),
        table_name=env_str("TABLE_NAME", "tickets_mesclados"),
        lookback_days=env_int("LOOKBACK_DAYS", 30),
        window_days=env_int("WINDOW_DAYS", 7),
        dry_run=env_bool("DRY_RUN", False),
    )
    if not cfg.movidesk_token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    client = MovideskClient(cfg.base_url, cfg.movidesk_token, timeout=60)

    conn = None
    if not cfg.dry_run:
        conn = pg_connect()
        ensure_table(conn, cfg.db_schema, cfg.table_name)

    end = utc_now()
    start = end - timedelta(days=cfg.lookback_days)
    step = timedelta(days=max(cfg.window_days, 1))

    total_fetched = 0
    total_upserted = 0

    cur = start
    while cur < end:
        nxt = min(cur + step, end)
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

            merged_at = parse_dt(rec.get("lastUpdate")) or utc_now()
            payload = psycopg2.extras.Json(rec, dumps=lambda o: json.dumps(o, ensure_ascii=False))

            for mid in merged_ids:
                batch.append((int(mid), principal_id, merged_at, payload))

            if len(batch) >= 4000:
                if not cfg.dry_run and conn is not None:
                    total_upserted += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)
                batch.clear()

        if batch and not cfg.dry_run and conn is not None:
            total_upserted += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)

        log.info("janela=%s..%s fetched=%s upserted=%s", cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d"), total_fetched, total_upserted)
        cur = nxt

    if conn is not None:
        conn.close()


if __name__ == "__main__":
    main()
