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


SCRIPT_VERSION = "sync_range_tickets_merged_v7_2026_01_30"


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"missing env var: {name}")
    return v


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    s = v.strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("script_version=%s", SCRIPT_VERSION)
    logging.info(
        "run_context github_run_id=%s github_job=%s github_ref=%s",
        os.getenv("GITHUB_RUN_ID"),
        os.getenv("GITHUB_JOB"),
        os.getenv("GITHUB_REF"),
    )


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
            return requests.get(url, params=params, timeout=timeout)
        except Exception as e:
            last_exc = e
            sleep_s = min(2 ** (attempt - 1), 30)
            logging.warning("http_error attempt=%s sleep=%ss err=%s", attempt, sleep_s, repr(e))
            time.sleep(sleep_s)
    raise RuntimeError(f"http failed after {max_retries} retries: {repr(last_exc)}")


def movidesk_get_merged(
    api_base: str,
    token: str,
    ticket_id: int,
    query_keys: Sequence[str],
    timeout: int,
    max_retries: int,
    debug: bool,
) -> Optional[Any]:
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


def parse_dt(v: Any) -> Optional[datetime.datetime]:
    if not v:
        return None
    if isinstance(v, datetime.datetime):
        return v if v.tzinfo else v.replace(tzinfo=datetime.timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        d = datetime.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=datetime.timezone.utc)
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
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def ensure_jsonb(v: Dict[str, Any]) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def extract_relations_from_obj(obj: Dict[str, Any], queried_id: int) -> List[MergedRelation]:
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
    )

    if obj.get("ticketId") is not None and (
        obj.get("mergedTicketId") is not None or obj.get("mergedTicketID") is not None
    ):
        try:
            dest = int(obj.get("ticketId"))
            src = int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
            return [MergedRelation(merged_ticket_id=src, merged_into_id=dest, merged_at=merged_at, raw=obj)]
        except Exception:
            return []

    merged_into = (
        obj.get("mergedIntoId")
        or obj.get("mergedIntoTicketId")
        or obj.get("mainTicketId")
        or obj.get("mainTicketID")
        or obj.get("principalTicketId")
        or obj.get("principalId")
        or obj.get("mergedInto")
    )
    if merged_into is not None:
        src_guess = obj.get("ticketId") or obj.get("id") or queried_id
        try:
            src = int(src_guess)
            dest = int(merged_into)
            return [MergedRelation(merged_ticket_id=src, merged_into_id=dest, merged_at=merged_at, raw=obj)]
        except Exception:
            return []

    merged_ids: List[int] = []
    for key in ("mergedTicketsIds", "mergedTicketsIDs", "mergedTicketsIdsList"):
        if key in obj:
            merged_ids = to_int_list(obj.get(key))
            break

    if merged_ids:
        dest_guess = obj.get("ticketId") or obj.get("ticketID") or queried_id
        try:
            dest = int(dest_guess)
            out: List[MergedRelation] = []
            for mid in merged_ids:
                out.append(MergedRelation(merged_ticket_id=int(mid), merged_into_id=dest, merged_at=merged_at, raw=obj))
            return out
        except Exception:
            return []

    return []


def extract_relations(payload: Any, queried_id: int) -> List[MergedRelation]:
    out: List[MergedRelation] = []
    if payload is None:
        return out

    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            for it in payload["data"]:
                if isinstance(it, dict):
                    out.extend(extract_relations_from_obj(it, queried_id))
        elif "mergedTickets" in payload and isinstance(payload["mergedTickets"], list):
            for it in payload["mergedTickets"]:
                if isinstance(it, dict):
                    out.extend(extract_relations_from_obj(it, queried_id))
        else:
            out.extend(extract_relations_from_obj(payload, queried_id))

    elif isinstance(payload, list):
        for it in payload:
            if isinstance(it, dict):
                out.extend(extract_relations_from_obj(it, queried_id))

    dedup: Dict[int, MergedRelation] = {}
    for r in out:
        dedup[int(r.merged_ticket_id)] = r
    return list(dedup.values())


def control_order_by(cols: Set[str]) -> str:
    order: List[str] = []
    if "data_fim" in cols:
        order.append("data_fim desc nulls last")
    if "data_inicio" in cols:
        order.append("data_inicio desc nulls last")
    order.append("id_inicial desc")
    order.append("ctid desc")
    return ", ".join(order)


def read_control(
    conn,
    schema: str,
    control_table: str,
    id_col: str,
    start_override: Optional[int],
    end_override: Optional[int],
) -> Tuple[int, int, Optional[int]]:
    cols = get_table_columns(conn, schema, control_table)
    order_by = control_order_by(cols)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"select id_inicial, id_final, {qident(id_col)} as ptr from {qname(schema, control_table)} order by {order_by} limit 1"
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"control row not found in {schema}.{control_table}")
        id_inicial = int(row["id_inicial"])
        id_final = int(row["id_final"])
        ptr = row["ptr"]
        ptr_i = int(ptr) if ptr is not None else None

    if start_override is not None:
        id_inicial = start_override
    if end_override is not None:
        id_final = end_override

    if id_inicial < id_final:
        raise RuntimeError(f"invalid control range id_inicial={id_inicial} < id_final={id_final}")

    return id_inicial, id_final, ptr_i


def update_last_processed(conn, schema: str, control_table: str, id_col: str, new_value: int) -> bool:
    cols = get_table_columns(conn, schema, control_table)
    order_by = control_order_by(cols)

    with conn.cursor() as cur:
        cur.execute(f"select ctid from {qname(schema, control_table)} order by {order_by} limit 1 for update")
        row = cur.fetchone()
        if not row:
            return False
        ctid = row[0]
        cur.execute(
            f"update {qname(schema, control_table)} set {qident(id_col)} = %s where ctid = %s",
            (new_value, ctid),
        )
        return cur.rowcount == 1


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


def delete_from_other_tables(
    conn,
    merged_ticket_id: int,
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = %s", (merged_ticket_id,))
        d1 = cur.rowcount
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = %s", (merged_ticket_id,))
        d2 = cur.rowcount
    return d1, d2


def build_batch(next_id: int, end_id: int, batch_size: int) -> List[int]:
    stop = max(end_id, next_id - batch_size + 1)
    return list(range(next_id, stop - 1, -1))


def main() -> None:
    setup_logging()

    dsn = env("NEON_DSN")
    token = env("MOVIDESK_TOKEN")

    api_base = env("MOVIDESK_API_BASE", os.getenv("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1"))

    schema = env("DB_SCHEMA", "visualizacao_resolvidos")
    control_table = env("CONTROL_TABLE", "range_scan_control")

    target_table = os.getenv("TARGET_TABLE") or os.getenv("TABLE_NAME") or "tickets_mesclados"

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
    total_limit = env_int("LIMIT", batch_size)

    rpm = env_float("RPM", 0.0)
    throttle_s = (60.0 / rpm) if rpm and rpm > 0 else 0.0

    max_runtime = env_int("MAX_RUNTIME_SEC", 0)

    http_timeout = env_int("HTTP_TIMEOUT", 60)
    http_retries = env_int("HTTP_RETRIES", 4)

    query_keys = [s.strip() for s in env("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if s.strip()]
    debug_ids = parse_csv_ints(os.getenv("DEBUG_TICKET_IDS", ""))

    dry_run = env_bool("DRY_RUN", False)

    conn = connect_db(dsn)
    conn.autocommit = False
    lock_key = "sync_range_tickets_merged"
    started = time.monotonic()
    last_req = 0.0

    try:
        if not try_advisory_lock(conn, lock_key):
            logging.warning("another_run_detected exiting")
            conn.rollback()
            return

        id_inicial, id_final, last_processed = read_control(conn, schema, control_table, id_col, start_override_i, end_override_i)

        if last_processed is None:
            next_id = id_inicial
        else:
            next_id = last_processed - 1

        logging.info(
            "begin id_inicial=%s id_final=%s last_processed=%s next_id=%s batch=%s limit=%s rpm=%.2f query_keys=%s debug_ids=%s dry_run=%s",
            id_inicial,
            id_final,
            last_processed,
            next_id,
            batch_size,
            total_limit,
            rpm,
            ",".join(query_keys),
            ",".join(map(str, sorted(debug_ids))) if debug_ids else "",
            dry_run,
        )

        total_checked = 0
        total_upsert = 0
        total_deleted = 0
        total_removed_other = 0

        while next_id >= id_final and total_checked < total_limit:
            if max_runtime and (time.monotonic() - started) >= max_runtime:
                logging.info("stop_by_max_runtime checked=%s", total_checked)
                break

            remaining = total_limit - total_checked
            this_batch_size = batch_size if remaining >= batch_size else remaining
            batch_ids = build_batch(next_id, id_final, this_batch_size)

            batch_checked = 0
            batch_upsert = 0
            batch_deleted = 0
            batch_removed_other = 0

            for tid in batch_ids:
                if max_runtime and (time.monotonic() - started) >= max_runtime:
                    break

                if throttle_s > 0:
                    now = time.monotonic()
                    delta = now - last_req
                    if last_req > 0 and delta < throttle_s:
                        time.sleep(throttle_s - delta)

                debug = tid in debug_ids
                payload = movidesk_get_merged(api_base, token, tid, query_keys, http_timeout, http_retries, debug)
                last_req = time.monotonic()

                rels = extract_relations(payload, tid)

                if debug:
                    logging.info("debug_ticket ticket_id=%s relations=%s", tid, len(rels))

                if not dry_run:
                    if rels:
                        batch_upsert += upsert_mesclados(conn, schema, target_table, rels)
                        for r in rels:
                            d1, d2 = delete_from_other_tables(
                                conn,
                                r.merged_ticket_id,
                                resolvidos_schema,
                                resolvidos_table,
                                abertos_schema,
                                abertos_table,
                            )
                            batch_removed_other += d1 + d2
                    else:
                        batch_deleted += delete_ticket_from_mesclados(conn, schema, target_table, tid)

                batch_checked += 1

            if batch_checked == 0:
                break

            if not dry_run:
                conn.commit()

            new_last_processed = batch_ids[batch_checked - 1]

            ok = True
            if not dry_run:
                ok = update_last_processed(conn, schema, control_table, id_col, new_last_processed)
                conn.commit()

            total_checked += batch_checked
            total_upsert += batch_upsert
            total_deleted += batch_deleted
            total_removed_other += batch_removed_other

            next_id = new_last_processed - 1

            logging.info(
                "progress next_id=%s last_processed=%s checked=%s upsert=%s deleted=%s removed_other=%s control_update=%s",
                next_id,
                new_last_processed,
                total_checked,
                total_upsert,
                total_deleted,
                total_removed_other,
                ok,
            )

            if not ok:
                logging.error("stopping_due_to_control_update_failure last_processed=%s", new_last_processed)
                return

            if max_runtime and (time.monotonic() - started) >= max_runtime:
                logging.info("stop_by_max_runtime checked=%s", total_checked)
                break

        logging.info(
            "done checked=%s upsert=%s deleted=%s removed_other=%s",
            total_checked,
            total_upsert,
            total_deleted,
            total_removed_other,
        )

    finally:
        try:
            advisory_unlock(conn, lock_key)
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
