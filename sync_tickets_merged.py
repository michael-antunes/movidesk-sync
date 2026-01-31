import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union, Set

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_window100_desc_child_query_disable_satisfacao_triggers_2026-01-31"

MovideskResponse = Union[Dict[str, Any], List[Any], None]


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
        return None


def to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        return int(v)
    except Exception:
        try:
            s = str(v).strip()
            return int(s) if s else None
        except Exception:
            return None


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def set_session_timeouts(conn, lock_timeout_ms: int) -> None:
    if lock_timeout_ms <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(f"set lock_timeout = '{int(lock_timeout_ms)}ms'")


def ensure_table(conn, schema: str, table: str) -> None:
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


def list_columns(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where lower(table_schema) = lower(%s)
              and lower(table_name) = lower(%s)
            order by ordinal_position
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


def resolve_id_column(conn, schema: str, table: str, preferred: str, log: logging.Logger) -> str:
    cols = list_columns(conn, schema, table)
    cols_lower = {c.lower(): c for c in cols}
    candidates: List[str] = []
    if preferred:
        candidates.append(preferred)
    candidates.extend(["ticket_id", "ticketid", "ticketId", "id"])
    for cand in candidates:
        key = cand.lower()
        if key in cols_lower:
            chosen = cols_lower[key]
            log.info("SOURCE_ID_COL resolved: preferred=%s chosen=%s", preferred, chosen)
            return chosen
    raise RuntimeError(f"Não achei coluna de ID em {schema}.{table}. Preferido='{preferred}'. Colunas disponíveis={cols}")


def get_max_id(conn, schema: str, table: str, col: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(f"select max({qident(col)}) from {qname(schema, table)}")
        v = cur.fetchone()[0]
    return to_int(v)


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]) -> int:
    if not rows:
        return 0
    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for r in rows:
        dedup[int(r[0])] = (int(r[0]), int(r[1]), r[2], r[3])
    rows2 = list(dedup.values())
    sql = f"""
    insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update
      set merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {qname(schema, table)}.merged_at),
          raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows2, page_size=500)
    return len(rows2)


def commit_with_retry(conn, fn, log: logging.Logger, max_retries: int = 6):
    for attempt in range(max_retries):
        try:
            result = fn()
            conn.commit()
            return result
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.DeadlockDetected) as e:
            conn.rollback()
            wait = min(30, 2 ** attempt)
            log.warning("DB lock/deadlock (%s). Retry em %ss (tentativa %d/%d).", e.__class__.__name__, wait, attempt + 1, max_retries)
            time.sleep(wait)
    raise RuntimeError("Falhou por lock/deadlock muitas vezes.")


def movidesk_get(
    sess: requests.Session,
    base_url: str,
    endpoint: str,
    token: str,
    params: Dict[str, Any],
    timeout: int,
) -> Tuple[MovideskResponse, Optional[int], str]:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    p = {"token": token, **params}
    last_status: Optional[int] = None
    last_text = ""
    for i in range(5):
        try:
            r = sess.get(url, params=p, timeout=timeout)
            last_status = r.status_code
            last_text = (r.text or "")[:2000]
            if r.status_code == 404:
                return None, 404, last_text
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1))
                continue
            if r.status_code != 200:
                return None, r.status_code, last_text
            try:
                return r.json(), 200, last_text
            except Exception:
                return None, 200, last_text
        except Exception:
            time.sleep(2 * (i + 1))
    return None, last_status, last_text


def extract_pairs(obj: Any, out: List[Tuple[int, int, Optional[datetime], Any]], depth: int = 0) -> None:
    if depth > 10:
        return
    if isinstance(obj, dict):
        merged_at = parse_dt(
            obj.get("mergedDate")
            or obj.get("mergedAt")
            or obj.get("performedAt")
            or obj.get("date")
            or obj.get("createdAt")
            or obj.get("createdDate")
            or obj.get("lastUpdate")
            or obj.get("last_update")
        )

        dest = to_int(obj.get("ticketId") or obj.get("ticketID"))
        src = to_int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
        if dest is not None and src is not None:
            out.append((src, dest, merged_at, obj))

        if "ticket" in obj and isinstance(obj.get("ticket"), dict):
            d = obj.get("ticket") or {}
            dest2 = to_int(d.get("id") or d.get("ticketId") or d.get("ticketID"))
            if "mergedTicket" in obj and isinstance(obj.get("mergedTicket"), dict):
                s = obj.get("mergedTicket") or {}
                src2 = to_int(s.get("id") or s.get("ticketId") or s.get("ticketID"))
                if dest2 is not None and src2 is not None:
                    out.append((src2, dest2, merged_at, obj))

        merged_into = (
            obj.get("mergedIntoId")
            or obj.get("mergedIntoTicketId")
            or obj.get("mergedToTicketId")
            or obj.get("mainTicketId")
            or obj.get("mainTicketID")
            or obj.get("principalTicketId")
            or obj.get("principalId")
        )
        if merged_into is not None:
            dest3 = to_int(merged_into)
            src3 = to_int(obj.get("ticketId") or obj.get("id"))
            if src3 is not None and dest3 is not None:
                out.append((src3, dest3, merged_at, obj))

        merged_ids = None
        for k in ("mergedTicketsIds", "mergedTicketsIDs", "mergedTickets", "mergedTicketsId", "mergedTicketsID"):
            if k in obj:
                merged_ids = obj.get(k)
                break
        if dest is not None and merged_ids is not None:
            if isinstance(merged_ids, list):
                for it in merged_ids:
                    if isinstance(it, dict):
                        mid = to_int(it.get("ticketId") or it.get("id") or it.get("ticketID"))
                    else:
                        mid = to_int(it)
                    if mid is not None:
                        out.append((mid, dest, merged_at, obj))

        for v in obj.values():
            extract_pairs(v, out, depth + 1)

    elif isinstance(obj, list):
        for it in obj:
            extract_pairs(it, out, depth + 1)


def extract_relations_from_merged(data: MovideskResponse, raw: Any) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    tmp: List[Tuple[int, int, Optional[datetime], Any]] = []
    extract_pairs(data, tmp, 0)
    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    payload = json_payload(raw)
    for (src, dest, merged_at, _obj) in tmp:
        if src is None or dest is None:
            continue
        dedup[int(src)] = (int(src), int(dest), merged_at, payload)
    return list(dedup.values())


def relations_for_child(rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]], child_id: int) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    c = int(child_id)
    return [r for r in rows if int(r[0]) == c or int(r[1]) == c]


def get_triggers(conn, schema: str, table: str) -> List[Tuple[str, str, str]]:
    rel = f"{schema}.{table}"
    with conn.cursor() as cur:
        cur.execute(
            """
            select t.tgname, ns.nspname as fn_schema, p.proname
            from pg_trigger t
            join pg_proc p on p.oid = t.tgfoid
            join pg_namespace ns on ns.oid = p.pronamespace
            where t.tgrelid = %s::regclass
              and not t.tgisinternal
            order by t.tgname
            """,
            (rel,),
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def disable_satisfacao_triggers(conn, schema: str, table: str, log: logging.Logger) -> List[str]:
    disabled: List[str] = []
    trgs = get_triggers(conn, schema, table)
    with conn.cursor() as cur:
        for (tgname, fn_schema, proname) in trgs:
            if fn_schema == "visualizacao_satisfacao" or "satisfacao" in (proname or "").lower():
                cur.execute(f"alter table {qname(schema, table)} disable trigger {qident(tgname)}")
                disabled.append(tgname)
    if disabled:
        conn.commit()
        log.info("triggers_disabled=%s", disabled)
    return disabled


def enable_triggers(conn, schema: str, table: str, triggers: List[str], log: logging.Logger) -> None:
    if not triggers:
        return
    with conn.cursor() as cur:
        for tgname in triggers:
            cur.execute(f"alter table {qname(schema, table)} enable trigger {qident(tgname)}")
    conn.commit()
    log.info("triggers_enabled=%s", triggers)


def main():
    log_level = env_str("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    db_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table_name = env_str("TABLE_NAME", "tickets_mesclados")

    source_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    source_table = env_str("SOURCE_TABLE", "tickets_suporte")
    source_id_col_pref = env_str("SOURCE_ID_COL", "ticket_id")

    rpm = env_float("RPM", 10.0)
    pause_seconds = env_int("PAUSE_SECONDS", 20)
    http_timeout = env_int("HTTP_TIMEOUT", 45)

    lock_timeout_ms = env_int("PG_LOCK_TIMEOUT_MS", 5000)
    lock_retries = env_int("PG_LOCK_RETRIES", 6)

    window_size = 100

    delay_between_requests = (60.0 / rpm) if rpm and rpm > 0 else 0.0

    log.info("script_version=%s window_size=%d rpm=%.2f", SCRIPT_VERSION, window_size, rpm)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    set_session_timeouts(conn, lock_timeout_ms)
    ensure_table(conn, db_schema, table_name)
    conn.commit()

    disabled_triggers: List[str] = []
    try:
        disabled_triggers = disable_satisfacao_triggers(conn, db_schema, table_name, log)

        source_id_col = resolve_id_column(conn, source_schema, source_table, source_id_col_pref, log)
        max_id_db = get_max_id(conn, source_schema, source_table, source_id_col)
        if not max_id_db or max_id_db <= 0:
            log.warning("max_id_db veio vazio/0. Abortando.")
            return

        start_id = int(max_id_db)
        end_id = max(1, start_id - window_size + 1)
        ids = list(range(start_id, end_id - 1, -1))

        log.info("max_id_db=%s start_id=%s end_id=%s", max_id_db, start_id, end_id)

        sess = requests.Session()

        batch_map: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
        total_checked = 0
        total_upsert = 0

        for i, ticket_id in enumerate(ids, start=1):
            total_checked += 1

            rows_final: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []
            status_used: Optional[int] = None
            key_used: str = "q"

            data, st, _txt = movidesk_get(sess, base_url, "tickets/merged", token, {"q": str(ticket_id)}, http_timeout)
            status_used = st
            raw = {"endpoint": "tickets/merged", "params": {"q": str(ticket_id)}, "data": data}
            if st == 200:
                rows = extract_relations_from_merged(data, raw)
                rows_final = relations_for_child(rows, ticket_id)

            if not rows_final:
                key_used = "ticketId"
                data, st, _txt = movidesk_get(sess, base_url, "tickets/merged", token, {"ticketId": str(ticket_id)}, http_timeout)
                status_used = st
                raw = {"endpoint": "tickets/merged", "params": {"ticketId": str(ticket_id)}, "data": data}
                if st == 200:
                    rows = extract_relations_from_merged(data, raw)
                    rows_final = relations_for_child(rows, ticket_id)

            if not rows_final:
                key_used = "id"
                data, st, _txt = movidesk_get(sess, base_url, "tickets/merged", token, {"id": str(ticket_id)}, http_timeout)
                status_used = st
                raw = {"endpoint": "tickets/merged", "params": {"id": str(ticket_id)}, "data": data}
                if st == 200:
                    rows = extract_relations_from_merged(data, raw)
                    rows_final = relations_for_child(rows, ticket_id)

            log.info(
                "resultado_ticket %d/%d ticket_id=%d status=%s key=%s rows=%d",
                i, len(ids), ticket_id, status_used, key_used, len(rows_final)
            )

            for r in rows_final:
                batch_map[int(r[0])] = r

            if len(batch_map) >= 1000:
                batch = list(batch_map.values())

                def do_upsert():
                    return upsert_rows(conn, db_schema, table_name, batch)

                n = commit_with_retry(conn, do_upsert, log=log, max_retries=lock_retries)
                total_upsert += int(n)
                batch_map.clear()

            if delay_between_requests > 0:
                time.sleep(delay_between_requests)

        if batch_map:
            batch = list(batch_map.values())

            def do_upsert_final():
                return upsert_rows(conn, db_schema, table_name, batch)

            n = commit_with_retry(conn, do_upsert_final, log=log, max_retries=lock_retries)
            total_upsert += int(n)
            batch_map.clear()

        log.info("done checked=%d upserted=%d window=[%d..%d]", total_checked, total_upsert, start_id, end_id)

        if pause_seconds > 0:
            time.sleep(pause_seconds)

    finally:
        try:
            enable_triggers(conn, db_schema, table_name, disabled_triggers, log)
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
