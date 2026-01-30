import os
import sys
import json
import time
import logging
import datetime
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import psycopg2
import psycopg2.extras
import requests


SCRIPT_VERSION = "sync_range_tickets_merged_v5_2026_01_30"


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


def now_utc_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("script_version=%s", SCRIPT_VERSION)
    logging.info("run_context github_run_id=%s github_job=%s github_ref=%s",
                 os.getenv("GITHUB_RUN_ID"), os.getenv("GITHUB_JOB"), os.getenv("GITHUB_REF"))


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
    return psycopg2.connect(dsn, connect_timeout=15, application_name=os.getenv("APPLICATION_NAME", "sync_range_tickets_merged"))


def get_table_columns(conn, schema: str, table: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select lower(column_name)
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
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
            r = requests.get(url, params=params, timeout=timeout)
            return r
        except Exception as e:
            last_exc = e
            sleep_s = min(2 ** (attempt - 1), 30)
            logging.warning("http_error attempt=%s sleep=%ss err=%s", attempt, sleep_s, repr(e))
            time.sleep(sleep_s)
    raise RuntimeError(f"http failed after {max_retries} retries: {repr(last_exc)}")


def movidesk_get_merged(api_base: str, token: str, ticket_id: int, query_keys: Sequence[str],
                       timeout: int, max_retries: int, debug: bool) -> Optional[Any]:
    url = api_base.rstrip("/") + "/tickets/merged"
    for key in query_keys:
        params = {"token": token, key: ticket_id}
        r = request_with_retry(url, params=params, timeout=timeout, max_retries=max_retries)
        if debug:
            logging.info("api_call url=%s key=%s ticket_id=%s status=%s", url, key, ticket_id, r.status_code)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                raise RuntimeError(f"invalid json for ticket_id={ticket_id} key={key}: {repr(e)}")
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
    return None


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
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.datetime.strptime(v, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt.astimezone(datetime.timezone.utc)
            except Exception:
                continue
    return None


def as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
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


def extract_relations_from_obj(obj: Dict[str, Any]) -> List[MergedRelation]:
    merged_ticket_id = None
    merged_into_id = None
    for k in ("mergedTicketId", "merged_ticket_id", "mergedTicketID", "ticketId", "ticket_id", "id", "TicketId"):
        if k in obj:
            merged_ticket_id = as_int(obj.get(k))
            if merged_ticket_id is not None:
                break
    for k in ("mergedIntoTicketId", "merged_into_id", "mergedIntoId", "merged_into", "mergedToTicketId", "mergedToId"):
        if k in obj:
            merged_into_id = as_int(obj.get(k))
            if merged_into_id is not None:
                break
    merged_at = None
    for k in ("mergedAt", "merged_at", "mergedDate", "merged_date", "mergedOn", "merged_on", "createdDate", "created_date"):
        if k in obj:
            merged_at = parse_dt(obj.get(k))
            if merged_at is not None:
                break
    if merged_ticket_id is None or merged_into_id is None:
        return []
    return [MergedRelation(merged_ticket_id=merged_ticket_id, merged_into_id=merged_into_id, merged_at=merged_at, raw=obj)]


def extract_relations(payload: Any) -> List[MergedRelation]:
    rels: List[MergedRelation] = []
    if payload is None:
        return rels
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                rels.extend(extract_relations_from_obj(item))
        return rels
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    rels.extend(extract_relations_from_obj(item))
            return rels
        if "mergedTickets" in payload and isinstance(payload["mergedTickets"], list):
            for item in payload["mergedTickets"]:
                if isinstance(item, dict):
                    rels.extend(extract_relations_from_obj(item))
            return rels
        rels.extend(extract_relations_from_obj(payload))
        return rels
    return rels


def ensure_jsonb(v: Dict[str, Any]) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def read_control(conn, schema: str, control_table: str, id_col: str,
                 start_override: Optional[int], end_override: Optional[int]) -> Tuple[int, int, Optional[int], Dict[str, Any]]:
    cols = get_table_columns(conn, schema, control_table)
    where_active = ""
    order_by = "ctid desc"
    if "data_fim" in cols:
        where_active = "where data_fim is null"
    if "data_inicio" in cols:
        order_by = "data_inicio desc nulls last, " + order_by
    select_cols = ["id_inicial", "id_final", id_col]
    select_cols_sql = ", ".join(qident(c) for c in select_cols if c in cols or c in ("id_inicial", "id_final", id_col))
    sql = f"select {select_cols_sql} from {qname(schema, control_table)} {where_active} order by {order_by} limit 1"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"control row not found in {schema}.{control_table} (active_only={bool(where_active)})")
        id_inicial = int(row.get("id_inicial"))
        id_final = int(row.get("id_final"))
        last_processed = row.get(id_col)
        if last_processed is not None:
            last_processed = int(last_processed)
        meta = dict(row)
    if start_override is not None:
        id_inicial = start_override
    if end_override is not None:
        id_final = end_override
    if id_inicial < id_final:
        raise RuntimeError(f"invalid control range id_inicial={id_inicial} < id_final={id_final}")
    return id_inicial, id_final, last_processed, meta


def update_last_processed(conn, schema: str, control_table: str, id_col: str, new_value: int) -> bool:
    cols = get_table_columns(conn, schema, control_table)
    where_active = ""
    order_by = "ctid desc"
    if "data_fim" in cols:
        where_active = "where data_fim is null"
    if "data_inicio" in cols:
        order_by = "data_inicio desc nulls last, " + order_by
    set_bits = [f"{qident(id_col)} = %s"]
    if "updated_at" in cols:
        set_bits.append("updated_at = now()")
    sql = f"""
        update {qname(schema, control_table)}
        set {", ".join(set_bits)}
        where ctid = (
            select ctid from {qname(schema, control_table)}
            {where_active}
            order by {order_by}
            limit 1
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql, (new_value,))
        if cur.rowcount == 1:
            return True
    logging.error("control_update_failed table=%s.%s id_col=%s new_value=%s", schema, control_table, id_col, new_value)
    return False


def upsert_mesclados(conn, schema: str, table: str, rels: List[MergedRelation]) -> int:
    if not rels:
        return 0
    sql = f"""
        insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
        values (%s, %s, %s, %s::jsonb)
        on conflict (ticket_id) do update
        set merged_into_id = excluded.merged_into_id,
            merged_at = excluded.merged_at,
            raw_payload = excluded.raw_payload
    """
    rows = [(r.merged_ticket_id, r.merged_into_id, r.merged_at, ensure_jsonb(r.raw)) for r in rels]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    return len(rows)


def delete_ticket_from_mesclados(conn, schema: str, table: str, ticket_id: int) -> int:
    sql = f"delete from {qname(schema, table)} where ticket_id = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_id,))
        return cur.rowcount


def delete_from_other_tables(conn, merged_ticket_id: int,
                            resolvidos_schema: str, resolvidos_table: str,
                            abertos_schema: str, abertos_table: str) -> Tuple[int, int]:
    deleted_resolvidos = 0
    deleted_abertos = 0
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = %s", (merged_ticket_id,))
        deleted_resolvidos = cur.rowcount
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = %s", (merged_ticket_id,))
        deleted_abertos = cur.rowcount
    return deleted_resolvidos, deleted_abertos


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

    conn = connect_db(dsn)
    conn.autocommit = False
    try:
        if not try_advisory_lock(conn, "sync_range_tickets_merged"):
            logging.warning("another_run_detected exiting")
            conn.rollback()
            return

        id_inicial, id_final, last_processed, meta = read_control(conn, schema, control_table, id_col, start_override_i, end_override_i)

        if last_processed is None:
            next_id = id_inicial
        else:
            next_id = last_processed - 1

        logging.info("begin id_inicial=%s id_final=%s last_processed=%s next_id=%s batch=%s query_keys=%s debug_ids=%s",
                     id_inicial, id_final, last_processed, next_id, batch_size, ",".join(query_keys), ",".join(map(str, sorted(debug_ids))) if debug_ids else "")

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
                debug = (tid in debug_ids)
                payload = movidesk_get_merged(api_base, token, tid, query_keys, http_timeout, http_retries, debug)
                rels = extract_relations(payload)

                if debug:
                    logging.info("debug_ticket ticket_id=%s relations=%s", tid, len(rels))

                if rels:
                    batch_upsert += upsert_mesclados(conn, schema, target_table, rels)
                    for r in rels:
                        d1, d2 = delete_from_other_tables(conn, r.merged_ticket_id, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table)
                        batch_removed_other += d1 + d2
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
                "progress next_id=%s last_processed=%s checked=%s upsert=%s deleted=%s removed_other=%s elapsed_s=%.1f control_update=%s",
                next_id, new_last_processed, total_checked, total_upsert, total_deleted, total_removed_other, elapsed, ok
            )

            if not ok:
                logging.error("stopping_due_to_control_update_failure last_processed=%s", new_last_processed)
                return

        logging.info("done checked=%s upsert=%s deleted=%s removed_other=%s", total_checked, total_upsert, total_deleted, total_removed_other)

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
