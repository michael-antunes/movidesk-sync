import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_range_tickets_merged_date_cache_no_404_2026-02-01"


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


def parse_ids_csv(v: Any) -> List[int]:
    if v is None:
        return []
    s = str(v).strip()
    if not s:
        return []
    out: List[int] = []
    for part in s.split(";"):
        part = part.strip()
        if not part:
            continue
        iv = to_int(part)
        if iv is not None:
            out.append(int(iv))
    return out


def req_json(sess: requests.Session, url: str, params: Dict[str, Any], timeout: int, attempts: int) -> Tuple[Optional[Dict[str, Any]], int]:
    last_status = 0
    for i in range(attempts):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 200:
                try:
                    j = r.json()
                    return j if isinstance(j, dict) else None, 200
                except Exception:
                    return None, 200
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("retry-after")
                if ra:
                    try:
                        time.sleep(max(1, int(float(ra))))
                    except Exception:
                        time.sleep(min(2 ** i, 10))
                else:
                    time.sleep(min(2 ** i, 10))
                continue
            return None, r.status_code
        except Exception:
            time.sleep(min(2 ** i, 10))
    return None, last_status


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=15,
        application_name=os.getenv("APPLICATION_NAME", "sync_range_tickets_merged"),
    )


def try_lock(conn, key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select pg_try_advisory_lock(hashtext(%s))", (key,))
        return bool(cur.fetchone()[0])


def unlock(conn, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_unlock(hashtext(%s))", (key,))


def ensure_mesclados_table(conn, schema: str, table: str) -> None:
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


def fetch_control(conn, schema: str, table: str, id_col: str) -> Tuple[str, int, int, Optional[int], datetime, datetime]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select ctid, id_inicial, id_final, {qident(id_col)}, data_inicio, data_fim
            from {qname(schema, table)}
            order by ctid desc
            limit 1
            for update
            """
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"control row not found in {schema}.{table}")
        ctid = row[0]
        id_inicial = int(row[1]) if row[1] is not None else None
        id_final = int(row[2]) if row[2] is not None else None
        id_atual = int(row[3]) if row[3] is not None else None
        data_inicio = row[4]
        data_fim = row[5]
        if id_inicial is None or id_final is None:
            raise RuntimeError("range_scan_control precisa de id_inicial e id_final")
        if id_inicial < id_final:
            raise RuntimeError(f"invalid range id_inicial={id_inicial} < id_final={id_final}")
        if not isinstance(data_inicio, datetime) or not isinstance(data_fim, datetime):
            raise RuntimeError("range_scan_control precisa de data_inicio e data_fim")
        if data_inicio.tzinfo is None:
            data_inicio = data_inicio.replace(tzinfo=timezone.utc)
        if data_fim.tzinfo is None:
            data_fim = data_fim.replace(tzinfo=timezone.utc)
        return ctid, id_inicial, id_final, id_atual, data_inicio, data_fim


def update_control_ptr(conn, schema: str, table: str, id_col: str, ctid: str, new_value: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"update {qname(schema, table)} set {qident(id_col)}=%s where ctid=%s",
            (int(new_value), ctid),
        )


def parse_total_pages(page_number: Any) -> Optional[int]:
    s = str(page_number or "").strip()
    if "of" not in s:
        return None
    left, right = s.split("of", 1)
    right = right.strip()
    try:
        v = int(right)
        return v if v > 0 else None
    except Exception:
        return None


def build_merged_cache(
    sess: requests.Session,
    base_url: str,
    token: str,
    start_date: str,
    end_date: str,
    max_pages: int,
    timeout: int,
    attempts: int,
    sleep_between_pages: float,
    runtime_deadline: float,
    log: logging.Logger,
) -> Dict[int, Tuple[int, Optional[datetime], Dict[str, Any]]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    cache: Dict[int, Tuple[int, Optional[datetime], Dict[str, Any]]] = {}
    total_pages: Optional[int] = None

    page = 1
    while page <= max_pages:
        if time.time() >= runtime_deadline:
            break

        params: Dict[str, Any] = {"token": token, "startDate": start_date, "endDate": end_date, "page": str(page)}
        data, st = req_json(sess, url, params, timeout=timeout, attempts=attempts)
        if st != 200 or not data:
            break

        if total_pages is None:
            total_pages = parse_total_pages(data.get("pageNumber"))
            if total_pages is None:
                total_pages = max_pages

        merged_list = data.get("mergedTickets")
        if not isinstance(merged_list, list) or not merged_list:
            break

        for item in merged_list:
            if not isinstance(item, dict):
                continue
            parent_id = to_int(item.get("ticketId"))
            if not parent_id:
                continue
            merged_at = parse_dt(item.get("lastUpdate"))
            child_ids = parse_ids_csv(item.get("mergedTicketsIds"))
            if not child_ids:
                continue

            payload = {"parentTicketId": str(parent_id), **item}

            for cid in child_ids:
                prev = cache.get(int(cid))
                if prev is None:
                    cache[int(cid)] = (int(parent_id), merged_at, payload)
                else:
                    prev_at = prev[1]
                    if prev_at is None and merged_at is not None:
                        cache[int(cid)] = (int(parent_id), merged_at, payload)
                    elif prev_at is not None and merged_at is not None and merged_at > prev_at:
                        cache[int(cid)] = (int(parent_id), merged_at, payload)

        log.info("merged_cache_page page=%d cache_size=%d", page, len(cache))

        if total_pages is not None and page >= total_pages:
            break

        page += 1
        if sleep_between_pages > 0:
            time.sleep(float(sleep_between_pages))

    return cache


def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Dict[str, Any]]]) -> int:
    if not rows:
        return 0

    dedup: Dict[int, Tuple[int, int, Optional[datetime], str]] = {}
    for child_id, parent_id, merged_at, payload in rows:
        if child_id is None or parent_id is None:
            continue
        cid = int(child_id)
        raw = json.dumps(payload, ensure_ascii=False)
        prev = dedup.get(cid)
        if prev is None:
            dedup[cid] = (cid, int(parent_id), merged_at, raw)
        else:
            prev_at = prev[2]
            if prev_at is None and merged_at is not None:
                dedup[cid] = (cid, int(parent_id), merged_at, raw)
            elif prev_at is not None and merged_at is not None and merged_at > prev_at:
                dedup[cid] = (cid, int(parent_id), merged_at, raw)

    vals = list(dedup.values())
    if not vals:
        return 0

    sql = f"""
    insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update
      set merged_into_id = excluded.merged_into_id,
          merged_at = excluded.merged_at,
          raw_payload = excluded.raw_payload
    where {qname(schema, table)}.merged_at is null
       or excluded.merged_at > {qname(schema, table)}.merged_at
       or {qname(schema, table)}.merged_into_id is distinct from excluded.merged_into_id
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, vals, page_size=1000)
    return len(vals)


def delete_from_other_tables(
    conn,
    ticket_ids: Set[int],
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    excluidos_schema: str,
    excluidos_table: str,
) -> Tuple[int, int, int]:
    if not ticket_ids:
        return 0, 0, 0
    ids = list(sorted({int(x) for x in ticket_ids}))
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = any(%s::bigint[])", (ids,))
        d_res = cur.rowcount
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = any(%s::bigint[])", (ids,))
        d_abe = cur.rowcount
        cur.execute(f"delete from {qname(excluidos_schema, excluidos_table)} where ticket_id = any(%s::bigint[])", (ids,))
        d_exc = cur.rowcount
    return d_res, d_abe, d_exc


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, env_str("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("sync_range_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    control_table = env_str("CONTROL_TABLE", "range_scan_control")
    id_col = env_str("ID_COL", "id_atual_merged")

    mesclados_schema = env_str("MESCLADOS_SCHEMA", schema)
    mesclados_table = env_str("MESCLADOS_TABLE", "tickets_mesclados")

    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos")
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail")
    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos")
    excluidos_schema = env_str("EXCLUIDOS_SCHEMA", "visualizacao_resolvidos")
    excluidos_table = env_str("EXCLUIDOS_TABLE", "tickets_excluidos")

    limit_ids = env_int("LIMIT_IDS", 300)
    max_pages_cache = env_int("MAX_PAGES_CACHE", 5)
    sleep_between_pages = env_float("SLEEP_BETWEEN_PAGES", 6.5)

    http_timeout = env_int("HTTP_TIMEOUT", 25)
    http_attempts = env_int("HTTP_ATTEMPTS", 4)

    max_runtime_sec = env_int("MAX_RUNTIME_SEC", 1000)
    safety_seconds = env_int("SAFETY_SECONDS", 20)

    started = time.time()
    runtime_deadline = started + float(max_runtime_sec) - float(safety_seconds)

    log.info(
        "script_version=%s limit_ids=%d max_pages_cache=%d timeout=%d max_runtime=%d",
        SCRIPT_VERSION,
        limit_ids,
        max_pages_cache,
        http_timeout,
        max_runtime_sec,
    )

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = pg_connect(dsn)
    conn.autocommit = False
    try:
        if not try_lock(conn, "sync_range_tickets_merged"):
            log.info("another_run_detected exiting")
            conn.rollback()
            return

        ensure_mesclados_table(conn, mesclados_schema, mesclados_table)
        conn.commit()

        ctid, id_inicial, id_final, id_atual, data_inicio, data_fim = fetch_control(conn, schema, control_table, id_col)
        conn.commit()

        start_date = data_inicio.date().isoformat()
        end_date = data_fim.date().isoformat()

        next_id = id_inicial if id_atual is None else int(id_atual) - 1
        if next_id > id_inicial:
            next_id = id_inicial
        if next_id < id_final:
            log.info("already_done next_id=%d id_final=%d", next_id, id_final)
            return

        log.info("range id_inicial=%d id_final=%d next_id=%d startDate=%s endDate=%s", id_inicial, id_final, next_id, start_date, end_date)

        cache = build_merged_cache(
            sess=sess,
            base_url=base_url,
            token=token,
            start_date=start_date,
            end_date=end_date,
            max_pages=max_pages_cache,
            timeout=http_timeout,
            attempts=http_attempts,
            sleep_between_pages=sleep_between_pages,
            runtime_deadline=runtime_deadline,
            log=log,
        )

        processed = 0
        inserted = 0
        removed_res = 0
        removed_abe = 0
        removed_exc = 0

        while next_id >= id_final and processed < limit_ids and time.time() < runtime_deadline:
            tid = int(next_id)
            log.info("checking_ticket ticket_id=%d", tid)

            hit = cache.get(tid)
            if hit is not None:
                parent_id, merged_at, payload = hit
                rows = [(tid, int(parent_id), merged_at, payload)]
                inserted += upsert_mesclados(conn, mesclados_schema, mesclados_table, rows)
                d1, d2, d3 = delete_from_other_tables(
                    conn,
                    {tid},
                    resolvidos_schema,
                    resolvidos_table,
                    abertos_schema,
                    abertos_table,
                    excluidos_schema,
                    excluidos_table,
                )
                removed_res += d1
                removed_abe += d2
                removed_exc += d3

            with conn.cursor() as cur:
                cur.execute(
                    f"select ctid from {qname(schema, control_table)} order by ctid desc limit 1 for update"
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError("control row missing during update")
                ctid2 = row[0]
                update_control_ptr(conn, schema, control_table, id_col, ctid2, tid)

            conn.commit()

            processed += 1
            next_id = tid - 1

        log.info(
            "done processed=%d inserted=%d removed_resolvidos=%d removed_abertos=%d removed_excluidos=%d next_id=%d cache_size=%d",
            processed,
            inserted,
            removed_res,
            removed_abe,
            removed_exc,
            next_id,
            len(cache),
        )

    finally:
        try:
            unlock(conn, "sync_range_tickets_merged")
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("fatal_error err=%s", repr(e))
        sys.exit(1)
