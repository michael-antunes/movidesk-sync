import os
import sys
import json
import time
import logging
import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import psycopg2
import psycopg2.extras
import requests


SCRIPT_VERSION = "sync_range_tickets_merged_range_control_2026-01-31"


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"missing env var: {name}")
    return v


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return int(v)


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("script_version=%s", SCRIPT_VERSION)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def parse_csv_ints(s: str) -> Set[int]:
    out: Set[int] = set()
    for part in (s or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            continue
    return out


@dataclass(frozen=True)
class MergedRelation:
    merged_ticket_id: int
    merged_into_id: int
    merged_at: Optional[datetime.datetime]
    raw: Dict[str, Any]


def connect_db(dsn: str):
    return psycopg2.connect(
        dsn,
        connect_timeout=15,
        application_name=os.getenv("APPLICATION_NAME", "sync_range_tickets_merged"),
    )


def get_table_columns(conn, schema: str, table: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "select lower(column_name) from information_schema.columns where table_schema = %s and table_name = %s",
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}


def try_advisory_lock(conn, key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("select pg_try_advisory_lock(hashtext(%s))", (key,))
        return bool(cur.fetchone()[0])


def advisory_unlock(conn, key: str) -> None:
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_unlock(hashtext(%s))", (key,))


def request_with_retry(url: str, params: Dict[str, Any], timeout: int, max_retries: int) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            return requests.get(url, params=params, timeout=timeout)
        except Exception as e:
            last_exc = e
            sleep_s = min(2 ** (attempt - 1), 30)
            logging.warning("http_error attempt=%s sleep=%ss err=%s", attempt, sleep_s, repr(e))
            time.sleep(sleep_s)
    raise RuntimeError(f"http failed after {max_retries} retries: {repr(last_exc)}")


def parse_dt(value: Any) -> Optional[datetime.datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        v = v.replace("Z", "+00:00")
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dtv = datetime.datetime.strptime(v, fmt)
                if dtv.tzinfo is None:
                    dtv = dtv.replace(tzinfo=datetime.timezone.utc)
                return dtv.astimezone(datetime.timezone.utc)
            except Exception:
                continue
        try:
            dtv = datetime.datetime.fromisoformat(v)
            if dtv.tzinfo is None:
                dtv = dtv.replace(tzinfo=datetime.timezone.utc)
            return dtv.astimezone(datetime.timezone.utc)
        except Exception:
            return None
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)
    return None


def as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            return int(v)
        except Exception:
            return None
    return None


def to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[int] = []
        for x in v:
            xi = as_int(x)
            if xi is not None:
                out.append(int(xi))
        return out
    if isinstance(v, str):
        parts = [p.strip() for p in v.replace(";", ",").split(",")]
        out2: List[int] = []
        for p in parts:
            if not p:
                continue
            try:
                out2.append(int(p))
            except Exception:
                continue
        return out2
    xi2 = as_int(v)
    return [int(xi2)] if xi2 is not None else []


def extract_relations_from_obj(obj: Dict[str, Any], fallback_queried_id: int) -> List[MergedRelation]:
    merged_at = parse_dt(
        obj.get("mergedDate")
        or obj.get("mergedAt")
        or obj.get("performedAt")
        or obj.get("date")
        or obj.get("createdAt")
        or obj.get("createdDate")
        or obj.get("lastUpdate")
        or obj.get("last_update")
        or obj.get("merged_on")
        or obj.get("mergedOn")
    )

    out: List[MergedRelation] = []

    if obj.get("ticketId") is not None and (obj.get("mergedTicketId") is not None or obj.get("mergedTicketID") is not None):
        try:
            dest = int(obj.get("ticketId"))
            src = int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
            out.append(MergedRelation(merged_ticket_id=src, merged_into_id=dest, merged_at=merged_at, raw=obj))
            return out
        except Exception:
            pass

    merged_into = (
        obj.get("mergedIntoId")
        or obj.get("mergedIntoTicketId")
        or obj.get("mainTicketId")
        or obj.get("mainTicketID")
        or obj.get("principalTicketId")
        or obj.get("principalId")
        or obj.get("mergedInto")
        or obj.get("merged_into_id")
    )
    if merged_into is not None:
        src_guess = obj.get("ticketId") or obj.get("id") or obj.get("ticket_id") or fallback_queried_id
        try:
            src = int(src_guess)
            dest = int(merged_into)
            out.append(MergedRelation(merged_ticket_id=src, merged_into_id=dest, merged_at=merged_at, raw=obj))
            return out
        except Exception:
            pass

    merged_ids: List[int] = []
    for key in ("mergedTicketsIds", "mergedTicketsIDs", "mergedTicketsIdsList"):
        if key in obj:
            merged_ids = to_int_list(obj.get(key))
            break

    if merged_ids:
        dest_guess = obj.get("ticketId") or obj.get("ticketID") or obj.get("id") or fallback_queried_id
        try:
            dest = int(dest_guess)
            for mid in merged_ids:
                out.append(MergedRelation(merged_ticket_id=int(mid), merged_into_id=dest, merged_at=merged_at, raw=obj))
            return out
        except Exception:
            pass

    return out


def extract_relations(payload: Any, fallback_queried_id: int) -> List[MergedRelation]:
    rels: List[MergedRelation] = []
    if payload is None:
        return rels
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                rels.extend(extract_relations_from_obj(item, fallback_queried_id))
        return rels
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    rels.extend(extract_relations_from_obj(item, fallback_queried_id))
            return rels
        if "mergedTickets" in payload and isinstance(payload["mergedTickets"], list):
            for item in payload["mergedTickets"]:
                if isinstance(item, dict):
                    rels.extend(extract_relations_from_obj(item, fallback_queried_id))
            return rels
        rels.extend(extract_relations_from_obj(payload, fallback_queried_id))
        return rels
    return rels


def parse_total_pages(payload: Dict[str, Any], current_page: int) -> int:
    pn = str(payload.get("pageNumber") or "").strip()
    if "of" in pn:
        parts = [p.strip() for p in pn.split("of", 1)]
        if len(parts) == 2:
            tp = as_int(parts[1])
            if tp and tp > 0:
                return int(tp)
    tp2 = as_int(payload.get("totalPages") or payload.get("pages") or payload.get("pageCount"))
    if tp2 and tp2 > 0:
        return int(tp2)
    return int(current_page)


def merged_fallback_scan(
    api_base: str,
    token: str,
    child_id: int,
    start_date: str,
    end_date: Optional[str],
    timeout: int,
    max_retries: int,
    max_pages: int,
    debug: bool,
) -> List[MergedRelation]:
    url = api_base.rstrip("/") + "/tickets/merged"
    page = 1
    scanned = 0
    child_id_i = int(child_id)

    while scanned < max_pages:
        params: Dict[str, Any] = {"token": token, "startDate": start_date, "page": str(page)}
        if end_date:
            params["endDate"] = end_date
        r = request_with_retry(url, params=params, timeout=timeout, max_retries=max_retries)
        if debug:
            logging.info("fallback_page page=%s status=%s", page, r.status_code)
        if r.status_code != 200:
            return []
        try:
            payload = r.json()
        except Exception:
            return []
        if not isinstance(payload, dict):
            return []

        merged_list = payload.get("mergedTickets")
        if not isinstance(merged_list, list):
            merged_list = []

        for item in merged_list:
            if not isinstance(item, dict):
                continue
            parent_id = as_int(item.get("ticketId"))
            if not parent_id:
                continue
            ids = parse_csv_ints(str(item.get("mergedTicketsIds") or ""))
            if child_id_i in ids:
                merged_at = parse_dt(item.get("lastUpdate")) or datetime.datetime.now(datetime.timezone.utc)
                raw = {"parentTicketId": str(parent_id), **item}
                return [MergedRelation(merged_ticket_id=child_id_i, merged_into_id=int(parent_id), merged_at=merged_at, raw=raw)]

        total_pages = parse_total_pages(payload, page)
        if page >= total_pages:
            return []

        page += 1
        scanned += 1
        time.sleep(0.2)

    return []


def movidesk_fetch_best(
    api_base: str,
    token: str,
    ticket_id: int,
    query_keys: Sequence[str],
    timeout: int,
    max_retries: int,
    fallback_enabled: bool,
    fallback_start_date: str,
    fallback_end_date: Optional[str],
    fallback_max_pages: int,
    debug: bool,
) -> Tuple[Optional[Any], List[MergedRelation], Optional[str]]:
    url = api_base.rstrip("/") + "/tickets/merged"
    best_payload: Optional[Any] = None
    best_key: Optional[str] = None
    last_status: int = 0

    for key in query_keys:
        params = {"token": token, key: ticket_id}
        r = request_with_retry(url, params=params, timeout=timeout, max_retries=max_retries)
        last_status = r.status_code
        if debug:
            logging.info("api_call key=%s ticket_id=%s status=%s", key, ticket_id, r.status_code)

        if r.status_code == 200:
            try:
                payload = r.json()
            except Exception as e:
                raise RuntimeError(f"invalid json for ticket_id={ticket_id} key={key}: {repr(e)}")
            rels = extract_relations(payload, ticket_id)
            if rels:
                return payload, rels, key
            if best_payload is None:
                best_payload = payload
                best_key = key
            continue

        if r.status_code in (400, 404):
            continue

        if r.status_code in (401, 403):
            raise RuntimeError(f"movidesk auth error status={r.status_code} ticket_id={ticket_id}")

        if 500 <= r.status_code <= 599:
            if debug:
                logging.warning("api_5xx ticket_id=%s status=%s body=%s", ticket_id, r.status_code, r.text[:500])
            continue

        if debug:
            logging.warning("api_unexpected ticket_id=%s status=%s body=%s", ticket_id, r.status_code, r.text[:500])

    if fallback_enabled:
        if last_status in (0, 400, 404) or best_payload is not None:
            rels2 = merged_fallback_scan(
                api_base,
                token,
                ticket_id,
                fallback_start_date,
                fallback_end_date,
                timeout,
                max_retries,
                fallback_max_pages,
                debug,
            )
            if rels2:
                return None, rels2, "fallback_startDate_page"

    if best_payload is not None:
        return best_payload, extract_relations(best_payload, ticket_id), best_key

    return None, [], None


def ensure_jsonb(v: Dict[str, Any]) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def _control_order_by(cols: Set[str]) -> str:
    bits: List[str] = []
    if "id" in cols:
        bits.append("id desc nulls last")
    bits.append("ctid desc")
    return ", ".join(bits)


def read_control(
    conn,
    schema: str,
    control_table: str,
    id_col: str,
    start_override: Optional[int],
    end_override: Optional[int],
) -> Tuple[int, int, Optional[int]]:
    cols = get_table_columns(conn, schema, control_table)
    order_by = _control_order_by(cols)
    with conn.cursor() as cur:
        cur.execute(
            f"select id_inicial, id_final, {qident(id_col)} from {qname(schema, control_table)} order by {order_by} limit 1"
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"control row not found in {schema}.{control_table}")
        id_inicial = int(row[0])
        id_final = int(row[1])
        last_processed = row[2]
        last_processed = int(last_processed) if last_processed is not None else None

    if start_override is not None:
        id_inicial = int(start_override)
    if end_override is not None:
        id_final = int(end_override)

    if id_inicial < id_final:
        raise RuntimeError(f"invalid control range id_inicial={id_inicial} < id_final={id_final}")

    return id_inicial, id_final, last_processed


def update_last_processed(conn, schema: str, control_table: str, id_col: str, new_value: int) -> bool:
    cols = get_table_columns(conn, schema, control_table)
    order_by = _control_order_by(cols)

    with conn.cursor() as cur:
        cur.execute(f"select ctid from {qname(schema, control_table)} order by {order_by} limit 1 for update")
        row = cur.fetchone()
        if not row:
            return False
        ctid = row[0]
        cur.execute(
            f"update {qname(schema, control_table)} set {qident(id_col)}=%s where ctid=%s",
            (int(new_value), ctid),
        )
        return cur.rowcount == 1


def upsert_mesclados(conn, schema: str, table: str, rels: List[MergedRelation]) -> int:
    if not rels:
        return 0
    sql = (
        f"insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload) "
        f"values (%s, %s, %s, %s::jsonb) "
        f"on conflict (ticket_id) do update set "
        f"merged_into_id = excluded.merged_into_id, "
        f"merged_at = excluded.merged_at, "
        f"raw_payload = excluded.raw_payload"
    )
    rows = [(r.merged_ticket_id, r.merged_into_id, r.merged_at, ensure_jsonb(r.raw)) for r in rels]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    return len(rows)


def delete_ticket_from_mesclados(conn, schema: str, table: str, ticket_id: int) -> int:
    sql = f"delete from {qname(schema, table)} where ticket_id = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (int(ticket_id),))
        return cur.rowcount


def delete_from_other_tables(
    conn,
    merged_ticket_id: int,
    merged_at: Optional[datetime.datetime],
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    excluidos_schema: str,
    excluidos_table: str,
) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        if merged_at is None:
            cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = %s", (int(merged_ticket_id),))
            d_res = cur.rowcount
            cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = %s", (int(merged_ticket_id),))
            d_abe = cur.rowcount
            cur.execute(f"delete from {qname(excluidos_schema, excluidos_table)} where ticket_id = %s", (int(merged_ticket_id),))
            d_exc = cur.rowcount
            return d_res, d_abe, d_exc

        cur.execute(
            f"""
            delete from {qname(resolvidos_schema, resolvidos_table)}
            where ticket_id = %s
              and coalesce(last_update, updated_at, 'epoch'::timestamptz) <= %s
            """,
            (int(merged_ticket_id), merged_at),
        )
        d_res = cur.rowcount

        cur.execute(
            f"""
            delete from {qname(abertos_schema, abertos_table)}
            where ticket_id = %s
              and coalesce(last_update, updated_at, 'epoch'::timestamptz) <= %s
            """,
            (int(merged_ticket_id), merged_at),
        )
        d_abe = cur.rowcount

        cur.execute(
            f"""
            delete from {qname(excluidos_schema, excluidos_table)}
            where ticket_id = %s
              and coalesce(
                    last_update,
                    nullif(raw->>'lastUpdate','')::timestamptz,
                    date_excluido,
                    synced_at,
                    'epoch'::timestamptz
                  ) <= %s
            """,
            (int(merged_ticket_id), merged_at),
        )
        d_exc = cur.rowcount

    return d_res, d_abe, d_exc


def build_batch(next_id: int, end_id: int, batch_size: int) -> List[int]:
    stop = max(end_id, next_id - batch_size + 1)
    return list(range(next_id, stop - 1, -1))


def main() -> None:
    setup_logging()

    dsn = env("NEON_DSN")
    token = env("MOVIDESK_TOKEN")
    api_base = env("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    schema = env("DB_SCHEMA", "visualizacao_resolvidos")
    control_table = env("CONTROL_TABLE", "range_scan_control")
    target_table = env("TARGET_TABLE", "tickets_mesclados")

    resolvidos_schema = env("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos")
    resolvidos_table = env("RESOLVIDOS_TABLE", "tickets_resolvidos_detail")
    abertos_schema = env("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = env("ABERTOS_TABLE", "tickets_abertos")
    excluidos_schema = env("EXCLUIDOS_SCHEMA", "visualizacao_resolvidos")
    excluidos_table = env("EXCLUIDOS_TABLE", "tickets_excluidos")

    id_col = env("ID_COL", "id_atual_merged")
    start_override = os.getenv("START_ID_OVERRIDE")
    end_override = os.getenv("END_ID_OVERRIDE")
    start_override_i = int(start_override) if start_override and start_override.strip() else None
    end_override_i = int(end_override) if end_override and end_override.strip() else None

    batch_size = env_int("BATCH_SIZE", 10)
    http_timeout = env_int("HTTP_TIMEOUT", 30)
    http_retries = env_int("HTTP_RETRIES", 4)

    query_keys = [s.strip() for s in env("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if s.strip()]
    debug_ids = parse_csv_ints(os.getenv("DEBUG_TICKET_IDS", ""))

    fallback_enabled = env_int("MERGED_FALLBACK_ENABLED", 1) != 0
    fallback_max_pages = env_int("MERGED_FALLBACK_MAX_PAGES", 50)
    fb_start = os.getenv("MERGED_FALLBACK_START_DATE")
    if not fb_start or fb_start.strip() == "":
        fb_start = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    fb_end = os.getenv("MERGED_FALLBACK_END_DATE")
    if fb_end and fb_end.strip() == "":
        fb_end = None

    conn = connect_db(dsn)
    conn.autocommit = False
    try:
        if not try_advisory_lock(conn, "sync_range_tickets_merged"):
            logging.warning("another_run_detected exiting")
            conn.rollback()
            return

        id_inicial, id_final, last_processed = read_control(conn, schema, control_table, id_col, start_override_i, end_override_i)
        next_id = id_inicial if last_processed is None else int(last_processed) - 1

        logging.info(
            "begin id_inicial=%s id_final=%s id_atual_merged=%s next_id=%s batch=%s query_keys=%s fallback_enabled=%s fb_start=%s fb_end=%s fb_pages=%s",
            id_inicial,
            id_final,
            last_processed,
            next_id,
            batch_size,
            ",".join(query_keys),
            int(fallback_enabled),
            fb_start,
            fb_end or "",
            fallback_max_pages,
        )

        total_checked = 0
        total_upsert = 0
        total_deleted = 0
        total_removed_other = 0
        loop_started = time.time()

        while next_id >= id_final:
            batch_ids = build_batch(next_id, id_final, batch_size)

            batch_checked = 0
            batch_upsert = 0
            batch_deleted = 0
            batch_removed_other = 0

            for tid in batch_ids:
                debug = tid in debug_ids

                _payload, rels, key_used = movidesk_fetch_best(
                    api_base,
                    token,
                    tid,
                    query_keys,
                    http_timeout,
                    http_retries,
                    fallback_enabled,
                    fb_start,
                    fb_end,
                    fallback_max_pages,
                    debug,
                )

                if debug:
                    logging.info("debug_ticket ticket_id=%s key_used=%s relations=%s", tid, key_used, len(rels))

                if rels:
                    batch_upsert += upsert_mesclados(conn, schema, target_table, rels)
                    for r in rels:
                        d1, d2, d3 = delete_from_other_tables(
                            conn,
                            r.merged_ticket_id,
                            r.merged_at,
                            resolvidos_schema,
                            resolvidos_table,
                            abertos_schema,
                            abertos_table,
                            excluidos_schema,
                            excluidos_table,
                        )
                        batch_removed_other += d1 + d2 + d3
                else:
                    batch_deleted += delete_ticket_from_mesclados(conn, schema, target_table, tid)

                batch_checked += 1

            conn.commit()

            new_last_processed = batch_ids[-1]
            ok = update_last_processed(conn, schema, control_table, id_col, new_last_processed)
            conn.commit()

            total_checked += batch_checked
            total_upsert += batch_upsert
            total_deleted += batch_deleted
            total_removed_other += batch_removed_other

            next_id = new_last_processed - 1

            elapsed = time.time() - loop_started
            logging.info(
                "progress next_id=%s id_atual_merged=%s checked=%s upsert=%s deleted=%s removed_other=%s elapsed_s=%.1f control_update=%s",
                next_id,
                new_last_processed,
                total_checked,
                total_upsert,
                total_deleted,
                total_removed_other,
                elapsed,
                ok,
            )

            if not ok:
                logging.error("stopping_due_to_control_update_failure id_atual_merged=%s", new_last_processed)
                return

        logging.info(
            "done checked=%s upsert=%s deleted=%s removed_other=%s",
            total_checked,
            total_upsert,
            total_deleted,
            total_removed_other,
        )

    finally:
        try:
            advisory_unlock(conn, "sync_range_tickets_merged")
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
