import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_api_candidates_v1_2026-01-27"


# -------------------------
# env helpers
# -------------------------
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


# -------------------------
# DB
# -------------------------
def ensure_tables(conn, merged_schema: str, merged_table: str, ctl_schema: str, ctl_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {qident(merged_schema)}")
        cur.execute(
            f"""
            create table if not exists {qname(merged_schema, merged_table)} (
              ticket_id bigint primary key,
              merged_into_id bigint not null,
              merged_at timestamptz,
              raw_payload jsonb
            )
            """
        )
        cur.execute(
            f"create index if not exists ix_{merged_table}_merged_into_id on {qname(merged_schema, merged_table)} (merged_into_id)"
        )
        cur.execute(
            f"create index if not exists ix_{merged_table}_merged_at on {qname(merged_schema, merged_table)} (merged_at)"
        )

        cur.execute(f"create schema if not exists {qident(ctl_schema)}")
        cur.execute(
            f"""
            create table if not exists {qname(ctl_schema, ctl_table)} (
              id smallint primary key default 1,
              cursor_ts timestamptz,
              cursor_ticket_id bigint,
              updated_at timestamptz not null default now()
            )
            """
        )
        cur.execute(
            f"""
            insert into {qname(ctl_schema, ctl_table)} (id, cursor_ts, cursor_ticket_id)
            values (1, null, null)
            on conflict (id) do nothing
            """
        )


def get_cursor(conn, ctl_schema: str, ctl_table: str) -> Tuple[Optional[dt.datetime], Optional[int]]:
    with conn.cursor() as cur:
        cur.execute(f"select cursor_ts, cursor_ticket_id from {qname(ctl_schema, ctl_table)} where id=1")
        row = cur.fetchone()
        if not row:
            return None, None
        ts = row[0]
        tid = row[1]
        if ts is not None and isinstance(ts, dt.datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return ts, int(tid) if tid is not None else None


def set_cursor(conn, ctl_schema: str, ctl_table: str, ts: dt.datetime, ticket_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {qname(ctl_schema, ctl_table)}
               set cursor_ts=%s,
                   cursor_ticket_id=%s,
                   updated_at=now()
             where id=1
            """,
            (ts, ticket_id),
        )


def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[dt.datetime], Any]]) -> int:
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


# -------------------------
# Movidesk API helpers
# -------------------------
def odata_dt_variants(ts: dt.datetime) -> List[str]:
    # tenta alguns formatos comuns de OData
    z = ts.astimezone(dt.timezone.utc).replace(microsecond=0)
    iso = z.isoformat().replace("+00:00", "Z")
    return [
        iso,  # 2026-01-27T12:34:56Z
        f"datetimeoffset'{iso}'",
        f"datetime'{iso}'",
    ]


def movidesk_get_json(sess: requests.Session, url: str, params: Dict[str, Any], timeout: int) -> Tuple[Any, int, str]:
    r = sess.get(url, params=params, timeout=timeout)
    txt = r.text or ""
    if r.status_code != 200:
        return None, r.status_code, txt[:4000]
    try:
        return r.json(), 200, ""
    except Exception:
        return None, 200, txt[:4000]


def list_updated_tickets_from_api(
    sess: requests.Session,
    base_url: str,
    token: str,
    since_ts: dt.datetime,
    limit: int,
    timeout: int,
    log: logging.Logger,
) -> Tuple[List[Tuple[int, dt.datetime]], str, str]:
    """
    Retorna lista de (ticket_id, updated_ts) vindo da API /tickets
    - tenta descobrir qual campo é o "updated" com fallback.
    - tenta alguns formatos de datetime no $filter.
    """
    tickets_url = f"{base_url.rstrip('/')}/tickets"

    id_fields = ["id", "ticketId", "ticket_id"]
    upd_fields = ["lastUpdate", "updatedDate", "updatedAt", "lastUpdateDate", "lastUpdated"]

    # paging OData
    top = max(1, min(100, limit))
    skip = 0

    last_err = ""
    chosen = ""

    # vamos tentar combinações até uma funcionar
    for idf in id_fields:
        for upf in upd_fields:
            for dt_expr in odata_dt_variants(since_ts):
                params = {
                    "token": token,
                    "$select": f"{idf},{upf}",
                    "$orderby": f"{upf} asc,{idf} asc",
                    "$top": str(top),
                    "$skip": str(skip),
                    "$filter": f"{upf} ge {dt_expr}",
                }

                data, status, errtxt = movidesk_get_json(sess, tickets_url, params, timeout)
                if status != 200:
                    last_err = f"status={status} err={errtxt}"
                    continue

                if not isinstance(data, list):
                    last_err = f"status=200 but not list (type={type(data)})"
                    continue

                out: List[Tuple[int, dt.datetime]] = []
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    tid_raw = item.get(idf)
                    upd_raw = item.get(upf)
                    try:
                        tid = int(tid_raw)
                    except Exception:
                        continue
                    upd = parse_dt(upd_raw)
                    if upd is None:
                        # se não vier data, ignora (mas mantém o tid pra debug)
                        continue
                    out.append((tid, upd))

                chosen = f"id_field={idf} upd_field={upf} filter='{upf} ge {dt_expr}'"
                return out[:limit], chosen, last_err

    return [], chosen, last_err


def fetch_merged_for_ticket(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
    query_keys: List[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    last_status = None
    last_err = ""

    for key in query_keys:
        params = {"token": token, key: str(ticket_id)}
        # retry leve
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
                    last_err = (r.text or "")[:4000]
                    return None, r.status_code
                data = r.json()
                return data if isinstance(data, dict) else None, 200
            except Exception as e:
                last_err = repr(e)
                time.sleep(2 * (i + 1))
        # tenta próxima key
        continue

    return None, last_status


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


def extract_merge_rows(queried_id: int, raw: Dict[str, Any]) -> List[Tuple[int, int, Optional[dt.datetime], Any]]:
    """
    Sempre grava: (ticket_id_filho, merged_into_id_pai, merged_at, raw_payload)
    """
    payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))

    merged_at = parse_dt(
        raw.get("mergedDate")
        or raw.get("mergedAt")
        or raw.get("performedAt")
        or raw.get("date")
        or raw.get("lastUpdate")
        or raw.get("last_update")
    )

    # cenário: consultou o pai e veio lista de filhos
    merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTickets"))
    if merged_ids:
        dest = raw.get("ticketId") or raw.get("id") or queried_id
        try:
            dest_i = int(dest)
        except Exception:
            dest_i = int(queried_id)
        out = []
        for mid in merged_ids:
            out.append((int(mid), dest_i, merged_at, payload))
        return out

    # cenário: consultou o filho e veio mergedIntoId
    merged_into = raw.get("mergedIntoId") or raw.get("mergedIntoTicketId") or raw.get("mainTicketId") or raw.get("principalTicketId")
    if merged_into is not None:
        try:
            return [(int(queried_id), int(merged_into), merged_at, payload)]
        except Exception:
            return []

    # cenário: layout tipo ticketId + mergedTicketId
    if raw.get("ticketId") is not None and (raw.get("mergedTicketId") is not None or raw.get("mergedTicketID") is not None):
        try:
            dest = int(raw.get("ticketId"))
            src = int(raw.get("mergedTicketId") or raw.get("mergedTicketID"))
            return [(src, dest, merged_at, payload)]
        except Exception:
            return []

    return []


# -------------------------
# main
# -------------------------
def main() -> None:
    log_level = env_str("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    merged_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    merged_table = env_str("TABLE_NAME", "tickets_mesclados")
    ctl_schema = env_str("CONTROL_SCHEMA", merged_schema)
    ctl_table = env_str("CONTROL_TABLE", "sync_control_tickets_merged_api")

    tickets_per_run = env_int("TICKETS_PER_RUN", 50)

    # cursor/overlap: evita perder evento que chegou "atrasado"
    bootstrap_lookback_hours = env_int("BOOTSTRAP_LOOKBACK_HOURS", 48)
    overlap_minutes = env_int("OVERLAP_MINUTES", 60)

    # rate limit (Movidesk default 10 rpm)
    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0

    http_timeout = env_int("HTTP_TIMEOUT", 45)
    query_keys = [k.strip() for k in env_str("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if k.strip()]

    log.info("script_version=%s tickets_per_run=%d rpm=%.2f", SCRIPT_VERSION, tickets_per_run, rpm)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        ensure_tables(conn, merged_schema, merged_table, ctl_schema, ctl_table)
        conn.commit()

        cursor_ts, cursor_tid = get_cursor(conn, ctl_schema, ctl_table)
        if cursor_ts is None:
            cursor_ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=bootstrap_lookback_hours)
            cursor_tid = 0

        since_ts = cursor_ts - dt.timedelta(minutes=overlap_minutes)

        # 1) pega candidatos direto da API /tickets (não do banco)
        candidates, chosen, last_err = list_updated_tickets_from_api(
            sess=sess,
            base_url=base_url,
            token=token,
            since_ts=since_ts,
            limit=tickets_per_run,
            timeout=http_timeout,
            log=log,
        )

        log.info("candidates_from_api n=%d since_ts=%s chosen=%s last_err=%s",
                 len(candidates), since_ts.isoformat(), chosen, (last_err or ""))

        if not candidates:
            # nada novo; mantém cursor e encerra (workflow rerun chama de novo)
            conn.commit()
            return

        # 2) para cada ticket, chama /tickets/merged
        status_counts: Dict[int, int] = {}
        rows_map: Dict[int, Tuple[int, int, Optional[dt.datetime], Any]] = {}

        checked = 0
        found = 0

        for tid, upd_ts in candidates:
            checked += 1

            raw, st = fetch_merged_for_ticket(sess, base_url, token, int(tid), http_timeout, query_keys)
            if st is not None:
                status_counts[st] = status_counts.get(st, 0) + 1

            if raw:
                rows = extract_merge_rows(int(tid), raw)
                for r in rows:
                    rows_map[r[0]] = r
                found += len(rows)

            if throttle > 0:
                time.sleep(throttle)

        # 3) grava no banco
        upserted = upsert_mesclados(conn, merged_schema, merged_table, list(rows_map.values()))

        # 4) avança cursor pro maior updated_ts que vimos (tie-break por ticket_id)
        candidates_sorted = sorted(candidates, key=lambda x: (x[1], x[0]))
        new_cursor_tid, new_cursor_ts = candidates_sorted[-1][0], candidates_sorted[-1][1]
        set_cursor(conn, ctl_schema, ctl_table, new_cursor_ts, int(new_cursor_tid))

        conn.commit()

        log.info("done checked=%d merges_found=%d upserted=%d new_cursor_ts=%s statuses=%s",
                 checked, found, upserted, new_cursor_ts.isoformat(), json.dumps(status_counts, ensure_ascii=False))

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
