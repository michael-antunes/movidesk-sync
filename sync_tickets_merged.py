import os
import time
import logging
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from zoneinfo import ZoneInfo


def env_str(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing env {name}")
        return default
    return v


def env_int(name: str, default: Optional[int] = None) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing env {name}")
        return int(default)
    return int(v)


def parse_total_pages(page_number: Any) -> Optional[int]:
    if page_number is None:
        return None
    s = str(page_number).strip()
    import re
    m = re.search(r"(\d+)\s*(?:of|de|/)\s*(\d+)", s, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(2))
        except Exception:
            return None
    return None


def parse_ids(v: Any) -> List[int]:
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
    s = str(v).strip()
    if not s:
        return []
    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def normalize_dt(v: Any, tz: dt.tzinfo) -> Optional[dt.datetime]:
    if not v:
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=tz)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        x = dt.datetime.fromisoformat(s)
        if x.tzinfo is None:
            x = x.replace(tzinfo=tz)
        return x.astimezone(dt.timezone.utc)
    except Exception:
        return None


@dataclass
class SyncCfg:
    token: str
    base_url: str
    neon_dsn: str
    schema: str = "visualizacao_resolvidos"
    table: str = "tickets_mesclados"
    lookback_days: int = 2
    window_days: int = 1
    rpm: int = 9
    page_size: int = 100
    batch_size: int = 2000
    max_depth: int = 3000


class MovideskClient:
    def __init__(self, cfg: SyncCfg):
        self.cfg = cfg
        self.sess = requests.Session()
        self.throttle_s = 60.0 / max(1, cfg.rpm)

    def _sleep_throttle(self):
        if self.throttle_s > 0:
            time.sleep(self.throttle_s)

    def get_merged_period(self, start_dt: dt.datetime, end_dt: dt.datetime, page: int) -> Dict[str, Any]:
        url = self.cfg.base_url.rstrip("/") + "/tickets/merged"
        params = {
            "token": self.cfg.token,
            "startDate": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDate": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "page": page,
            "pageSize": self.cfg.page_size,
        }
        last_err = None
        for _ in range(6):
            try:
                r = self.sess.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    self._sleep_throttle()
                    return r.json()
                if r.status_code == 404:
                    self._sleep_throttle()
                    return {"mergedTickets": [], "pageNumber": "1 of 1"}
                if r.status_code in (429, 503, 502, 504):
                    time.sleep(10)
                    last_err = f"{r.status_code} {r.text[:200]}"
                    continue
                last_err = f"{r.status_code} {r.text[:500]}"
                break
            except Exception as e:
                last_err = str(e)
                time.sleep(5)
        raise RuntimeError(last_err or "Falha Movidesk")

    def iter_merged(self, start_dt: dt.datetime, end_dt: dt.datetime) -> Iterator[Dict[str, Any]]:
        page = 1
        total_pages: Optional[int] = None
        while page <= self.cfg.max_depth:
            data = self.get_merged_period(start_dt, end_dt, page)
            items = data.get("mergedTickets") or data.get("items") or []
            if not items:
                break
            for it in items:
                yield it
            if total_pages is None:
                total_pages = parse_total_pages(data.get("pageNumber"))
            if total_pages is not None and page >= total_pages:
                break
            if self.cfg.page_size and len(items) < self.cfg.page_size:
                break
            page += 1


def ensure_table(conn, cfg: SyncCfg):
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {cfg.schema}")
        cur.execute(
            f"""
            create table if not exists {cfg.schema}.{cfg.table}(
                ticket_id bigint primary key,
                merged_into_id bigint not null,
                merged_at timestamptz,
                raw_payload jsonb
            )
            """
        )
        cur.execute(f"create index if not exists ix_tickets_mesclados_merged_into on {cfg.schema}.{cfg.table}(merged_into_id)")
    conn.commit()


def upsert_rows(conn, cfg: SyncCfg, rows: List[Tuple[Any, ...]]):
    sql = f"""
    insert into {cfg.schema}.{cfg.table}
      (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update set
      merged_into_id = excluded.merged_into_id,
      merged_at = excluded.merged_at,
      raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    token = env_str("MOVIDESK_TOKEN")
    base_url = os.getenv("MOVIDESK_BASE_URL") or os.getenv("MOVIDESK_API_BASE") or "https://api.movidesk.com/public/v1"
    neon_dsn = env_str("NEON_DSN")

    lookback_days = env_int("LOOKBACK_DAYS", env_int("DIAS_ATRAS", 2))
    window_days = env_int("WINDOW_DAYS", env_int("JANELA_DIAS", 1))
    rpm = env_int("RPM", 9)
    page_size = env_int("PAGE_SIZE", 100)
    batch_size = env_int("BATCH_SIZE", 2000)

    cfg = SyncCfg(
        token=token,
        base_url=base_url,
        neon_dsn=neon_dsn,
        lookback_days=lookback_days,
        window_days=window_days,
        rpm=rpm,
        page_size=page_size,
        batch_size=batch_size,
    )

    tz = ZoneInfo("America/Sao_Paulo")
    now_local = dt.datetime.now(tz)
    start_day = (now_local - dt.timedelta(days=cfg.lookback_days)).date()
    start = dt.datetime.combine(start_day, dt.time(0, 0, 0), tzinfo=tz)
    end = now_local

    client = MovideskClient(cfg)

    with psycopg2.connect(cfg.neon_dsn) as conn:
        ensure_table(conn, cfg)

        cur_dt = start
        total_fetched = 0
        total_upserted = 0

        while cur_dt < end:
            nxt = min(end, cur_dt + dt.timedelta(days=cfg.window_days))

            fetched = 0
            upserted = 0
            batch: List[Tuple[Any, ...]] = []

            logging.info("janela=%s..%s", cur_dt.isoformat(), nxt.isoformat())

            for rec in client.iter_merged(cur_dt, nxt):
                fetched += 1

                principal_id = rec.get("id") or rec.get("ticketId") or rec.get("ticket_id")
                if principal_id is None:
                    continue
                try:
                    principal_id_int = int(principal_id)
                except Exception:
                    continue

                merged_ids = parse_ids(rec.get("mergedTicketsIds") or rec.get("mergedTicketsIDs") or rec.get("mergedTicketsIdsList"))
                merged_at = normalize_dt(rec.get("mergedAt") or rec.get("mergedDate") or rec.get("merged_at") or rec.get("date") or rec.get("performedAt"), tz)

                if merged_ids:
                    for mid in merged_ids:
                        batch.append((int(mid), principal_id_int, merged_at, Json(rec)))
                else:
                    merged_tickets = rec.get("mergedTickets")
                    if isinstance(merged_tickets, list):
                        for it in merged_tickets:
                            if isinstance(it, dict):
                                mid = it.get("id") or it.get("ticketId") or it.get("ticket_id")
                                if mid is not None:
                                    try:
                                        batch.append((int(mid), principal_id_int, merged_at, Json(rec)))
                                    except Exception:
                                        pass

                if len(batch) >= cfg.batch_size:
                    upsert_rows(conn, cfg, batch)
                    upserted += len(batch)
                    batch.clear()

            if batch:
                upsert_rows(conn, cfg, batch)
                upserted += len(batch)
                batch.clear()

            logging.info("janela_result fetched=%s upserted=%s", fetched, upserted)

            total_fetched += fetched
            total_upserted += upserted
            cur_dt = nxt

        logging.info("total fetched=%s upserted=%s", total_fetched, total_upserted)


if __name__ == "__main__":
    main()
