import os
import json
import time
import logging
import datetime as dt
import re
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


_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qident(s: str) -> str:
    if not _SAFE_IDENT.match(s):
        raise ValueError(f"Unsafe identifier: {s!r}")
    return f'"{s}"'


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
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def unique_preserve_order(xs: List[int]) -> List[int]:
    seen = set()
    out = []
    for x in xs:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


class NeonDB:
    def __init__(self, dsn: str, log: logging.Logger, lock_timeout_ms: int):
        self.dsn = dsn
        self.log = log
        self.lock_timeout_ms = max(0, int(lock_timeout_ms))
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.last_ping = 0.0

    def connect(self):
        if self.conn is not None and getattr(self.conn, "closed", 1) == 0:
            return
        self.conn = psycopg2.connect(
            self.dsn,
            connect_timeout=20,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        self.conn.autocommit = False
        self.last_ping = time.monotonic()

    def close(self):
        try:
            if self.conn is not None and getattr(self.conn, "closed", 1) == 0:
                self.conn.close()
        finally:
            self.conn = None

    def ping_if_needed(self, ping_every_sec: int):
        if ping_every_sec <= 0:
            return
        now = time.monotonic()
        if now - self.last_ping < ping_every_sec:
            return
        try:
            self.connect()
            with self.conn.cursor() as cur:
                cur.execute("select 1")
            self.conn.commit()
            self.last_ping = now
        except Exception:
            try:
                if self.conn is not None:
                    self.conn.rollback()
            except Exception:
                pass
            self.close()
            self.connect()
            with self.conn.cursor() as cur:
                cur.execute("select 1")
            self.conn.commit()
            self.last_ping = now

    def ensure_table(self, schema: str, table: str):
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
            cur.execute(
                f"create index if not exists ix_tickets_mesclados_merged_into on {qname(schema, table)} (merged_into_id)"
            )
        self.conn.commit()

    def fetch_recently_updated_ticket_ids(
        self,
        src_schema: str,
        src_table: str,
        src_id_col: str,
        src_date_col: str,
        since_utc: dt.datetime,
        limit: int,
    ) -> List[int]:
        """
        Pega candidatos por *mais recentemente atualizados*, NÃO por ticket_id.
        Isso evita perder mescla em ticket menor (ex.: 303009) mesmo com muitos tickets maiores.
        """
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                select t.ticket_id
                from (
                  select {qident(src_id_col)}::bigint as ticket_id,
                         max({qident(src_date_col)}) as last_upd
                  from {qname(src_schema, src_table)}
                  where {qident(src_date_col)} >= %s
                  group by {qident(src_id_col)}
                ) t
                order by t.last_upd desc, t.ticket_id desc
                limit %s
                """,
                (since_utc, limit),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
        self.conn.commit()
        return ids

    def fetch_max_ticket_id(self, src_schema: str, src_table: str, src_id_col: str) -> Optional[int]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(f"select max({qident(src_id_col)}) from {qname(src_schema, src_table)}")
            row = cur.fetchone()
        self.conn.commit()
        if not row or row[0] is None:
            return None
        try:
            return int(row[0])
        except Exception:
            return None

    def upsert_rows(self, schema: str, table: str, rows: List[Tuple[int, int, Optional[dt.datetime], Any]], retries: int) -> int:
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
        for attempt in range(max(1, retries)):
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    if self.lock_timeout_ms > 0:
                        cur.execute(f"set local lock_timeout = '{int(self.lock_timeout_ms)}ms'")
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


def movidesk_get_merged(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"

    # Tenta os 3 formatos porque o manual menciona ticketId e q, e tem casos que aceitam id.
    for key in ("ticketId", "id", "q"):
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
    """
    Sai sempre no formato da sua tabela:
      ticket_id        = o ticket que foi mesclado (filho)
      merged_into_id   = o ticket principal (pai)
    """
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

    # Caso 1: consultou o ticket principal e veio lista de tickets mesclados nele
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

    # Caso 2: consultou o ticket filho e veio o "mergedIntoId"
    merged_into = (
        raw.get("mergedIntoId")
        or raw.get("mergedIntoTicketId")
        or raw.get("mainTicketId")
        or raw.get("principalTicketId")
        or raw.get("principalId")
        or raw.get("mergedInto")
    )
    if merged_into is not None:
        try:
            rows.append((int(queried_id), int(merged_into), merged_at, payload))
        except Exception:
            pass

    return rows


def pick_row(
    old: Optional[Tuple[int, int, Optional[dt.datetime], Any]],
    new: Tuple[int, int, Optional[dt.datetime], Any],
):
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
    log = logging.getLogger("merged-toploop")

    token = env_str("MOVIDESK_TOKEN")
    base_url = env_str("MOVIDESK_BASE_URL", env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1"))
    neon_dsn = env_str("NEON_DSN")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table = env_str("TABLE_NAME", "tickets_mesclados")

    src_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    src_table = env_str("SOURCE_TABLE", "tickets_suporte")
    src_id_col = env_str("SOURCE_ID_COL", "ticket_id")
    src_date_col = env_str("SOURCE_DATE_COL", "updated_at")

    tickets_per_run = env_int("TICKETS_PER_RUN", 20)
    lookback_hours = env_int("LOOKBACK_HOURS", 48)

    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    http_timeout = env_int("HTTP_TIMEOUT", 45)

    pause_seconds = env_int("PAUSE_SECONDS", 20)
    lock_timeout_ms = env_int("LOCK_TIMEOUT_MS", 5000)
    lock_retries = env_int("LOCK_RETRIES", 6)

    log_every = env_int("LOG_EVERY", 1)
    ping_every = env_int("DB_PING_EVERY", 30)

    since_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)

    db = NeonDB(neon_dsn, log, lock_timeout_ms=lock_timeout_ms)
    db.ensure_table(schema, table)

    # 1) candidatos por updated_at (resolve o problema do 303009)
    recent_ids = db.fetch_recently_updated_ticket_ids(
        src_schema, src_table, src_id_col, src_date_col, since_utc, limit=max(1, tickets_per_run)
    )

    # 2) fallback: top IDs pelo max(ticket_id) (caso o updated_at não reflita algo)
    max_id = db.fetch_max_ticket_id(src_schema, src_table, src_id_col)
    top_ids: List[int] = []
    if max_id is not None:
        top_ids = [max_id - i for i in range(max(1, tickets_per_run)) if (max_id - i) > 0]

    # lista final, dedup, e limita
    ids = unique_preserve_order(recent_ids + top_ids)[: max(1, tickets_per_run)]

    if not ids:
        log.info("no_candidates since_utc=%s", since_utc.isoformat())
        db.close()
        return

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    checked = 0
    found = 0
    upserted_total = 0
    status_counts: Dict[int, int] = {}

    rows_map: Dict[int, Tuple[int, int, Optional[dt.datetime], Any]] = {}

    log.info(
        "run_begin candidates=%s since_utc=%s max_id=%s selected=%s",
        len(recent_ids),
        since_utc.isoformat(),
        max_id,
        ids,
    )

    for ticket_id in ids:
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

        db.ping_if_needed(ping_every)

        if checked % max(1, log_every) == 0:
            log.info("progress checked=%s found=%s unique_buf=%s last_id=%s", checked, found, len(rows_map), ticket_id)

    if rows_map:
        upserted_total += db.upsert_rows(schema, table, list(rows_map.values()), retries=max(1, lock_retries))
        rows_map.clear()

    log.info(
        "run_end checked=%s found=%s upserted=%s statuses=%s",
        checked,
        found,
        upserted_total,
        json.dumps(status_counts, ensure_ascii=False),
    )

    db.close()

    # pausa antes de encerrar (o workflow dá rerun sozinho)
    if pause_seconds > 0:
        log.info("sleep pause_seconds=%s", pause_seconds)
        time.sleep(pause_seconds)


if __name__ == "__main__":
    main()
