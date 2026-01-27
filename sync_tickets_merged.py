import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "top_merged_loop_v2_2026-01-27"


# -------------------------
# Env helpers
# -------------------------

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


def env_float(k: str, default: float) -> float:
    v = env_str(k)
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def env_bool(k: str, default: bool = False) -> bool:
    v = env_str(k)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on", "sim")


def qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


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


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


# -------------------------
# DB helpers (timeouts / retry)
# -------------------------

def set_session_timeouts(conn, lock_timeout_ms: int) -> None:
    if lock_timeout_ms <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(f"set lock_timeout = '{int(lock_timeout_ms)}ms'")


def commit_with_retry(conn, fn, log: logging.Logger, max_retries: int = 6):
    for attempt in range(max_retries):
        try:
            result = fn()
            conn.commit()
            return result
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.DeadlockDetected) as e:
            conn.rollback()
            wait = min(30, 2 ** attempt)
            log.warning(
                "DB lock/deadlock (%s). Retry em %ss (tentativa %d/%d).",
                e.__class__.__name__,
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
    raise RuntimeError("Falhou por lock/deadlock muitas vezes. Tente novamente com o DB mais livre.")


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


def get_max_ticket_id(conn) -> int:
    """
    Fonte FIXA do max_id:
      dados_gerais.tickets_suporte (coluna id)
    """
    with conn.cursor() as cur:
        cur.execute('select max("id")::bigint from "dados_gerais"."tickets_suporte"')
        r = cur.fetchone()
        if not r or r[0] is None:
            raise RuntimeError("Não achei MAX(id) em dados_gerais.tickets_suporte")
        return int(r[0])


# -------------------------
# Movidesk API (/tickets/merged)
# -------------------------

MovideskResponse = Union[Dict[str, Any], List[Any], None]


def movidesk_get(
    sess: requests.Session,
    base_url: str,
    token: str,
    params: Dict[str, Any],
    timeout: int,
) -> Tuple[MovideskResponse, Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    p = {"token": token, **params}
    last_status: Optional[int] = None
    for i in range(5):
        try:
            r = sess.get(url, params=p, timeout=timeout)
            last_status = r.status_code

            if r.status_code == 404:
                return None, 404

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1))
                continue

            if r.status_code != 200:
                return None, r.status_code

            try:
                return r.json(), 200
            except Exception:
                return None, 200
        except Exception:
            time.sleep(2 * (i + 1))

    return None, last_status


def extract_relations_from_obj(obj: Dict[str, Any], fallback_queried_id: int) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    payload = json_payload(obj)
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

    out: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []

    # { ticketId: destino, mergedTicketId: origem }
    if obj.get("ticketId") is not None and (obj.get("mergedTicketId") is not None or obj.get("mergedTicketID") is not None):
        try:
            dest = int(obj.get("ticketId"))
            src = int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
            out.append((src, dest, merged_at, payload))
            return out
        except Exception:
            pass

    # { mergedIntoId: destino } (quando consultamos o filho)
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
        src_guess = obj.get("ticketId") or obj.get("id") or fallback_queried_id
        try:
            src = int(src_guess)
            dest = int(merged_into)
            out.append((src, dest, merged_at, payload))
            return out
        except Exception:
            pass

    # lista de filhos no pai (mergedTicketsIds)
    merged_ids: List[int] = []
    for key in ("mergedTicketsIds", "mergedTicketsIDs", "mergedTicketsIdsList"):
        if key in obj:
            merged_ids = to_int_list(obj.get(key))
            break

    if not merged_ids:
        mt = obj.get("mergedTickets") or obj.get("mergedTicketsList")
        if isinstance(mt, list):
            tmp: List[int] = []
            for it in mt:
                if isinstance(it, dict):
                    for k in ("id", "ticketId", "ticketID", "ticket_id"):
                        if k in it:
                            try:
                                tmp.append(int(it[k]))
                            except Exception:
                                pass
                            break
            merged_ids = tmp

    if merged_ids:
        dest_guess = obj.get("ticketId") or obj.get("ticketID") or fallback_queried_id
        try:
            dest = int(dest_guess)
            for mid in merged_ids:
                out.append((int(mid), dest, merged_at, payload))
            return out
        except Exception:
            pass

    return out


def extract_relations(data: MovideskResponse, queried_id: int) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    if data is None:
        return []
    out: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []
    if isinstance(data, dict):
        out.extend(extract_relations_from_obj(data, queried_id))
    elif isinstance(data, list):
        for it in data:
            if isinstance(it, dict):
                out.extend(extract_relations_from_obj(it, queried_id))

    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for r in out:
        dedup[int(r[0])] = r
    return list(dedup.values())


def fetch_merged_relations(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    all_rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []
    for params in (
        {"ticketId": str(ticket_id)},
        {"id": str(ticket_id)},
        {"q": str(ticket_id)},
    ):
        data, st = movidesk_get(sess, base_url, token, params, timeout)
        if st == 200 and data is not None:
            all_rows.extend(extract_relations(data, ticket_id))

    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for r in all_rows:
        dedup[int(r[0])] = r
    return list(dedup.values())


# -------------------------
# Writes
# -------------------------

def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]) -> int:
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


def delete_from_other_tables(
    conn,
    do_delete: bool,
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    ids: List[int],
) -> int:
    if not do_delete or not ids:
        return 0
    uniq = sorted(set(int(x) for x in ids))
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = any(%s)", (uniq,))
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = any(%s)", (uniq,))
    return len(uniq)


# -------------------------
# Main
# -------------------------

def main() -> None:
    log_level = (env_str("LOG_LEVEL", "INFO") or "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_top_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN") or env_str("NEON_DATABASE_URL") or env_str("DATABASE_URL")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1") or "https://api.movidesk.com/public/v1"

    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não informado")
    if not dsn:
        raise RuntimeError("NEON_DSN/NEON_DATABASE_URL não informado")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    table_mesclados = env_str("TABLE_NAME", "tickets_mesclados") or "tickets_mesclados"

    do_delete = env_bool("DELETE_FROM_BASE_TABLES", True)
    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail") or "tickets_resolvidos_detail"
    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual") or "visualizacao_atual"
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos") or "tickets_abertos"

    batch_size = env_int("BATCH_SIZE", 50)
    rpm = env_float("RPM", 10.0)
    http_timeout = env_int("HTTP_TIMEOUT", 60)
    lock_timeout_ms = env_int("LOCK_TIMEOUT_MS", 5000)
    write_retries = env_int("WRITE_RETRIES", 6)
    sleep_after_run = env_int("SLEEP_AFTER_RUN_SEC", 20)

    throttle = 0.0
    if rpm > 0:
        throttle = 60.0 / float(rpm)

    log.info("script_version=%s", SCRIPT_VERSION)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        set_session_timeouts(conn, lock_timeout_ms)
        ensure_table(conn, schema, table_mesclados)
        conn.commit()

        max_id = get_max_ticket_id(conn)
        start = max_id
        end = max_id - max(1, batch_size) + 1
        ids = list(range(start, end - 1, -1))
        log.info("max_id=%s scanning=%s..%s (n=%d) source=dados_gerais.tickets_suporte", max_id, start, end, len(ids))

        rows_map: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
        del_ids: List[int] = []

        for tid in ids:
            rows = fetch_merged_relations(sess, base_url, token, int(tid), http_timeout)
            for (src, dest, merged_at, payload) in rows:
                rows_map[int(src)] = (int(src), int(dest), merged_at, payload)
                del_ids.append(int(src))

            if throttle > 0:
                time.sleep(throttle)

        def _do_write():
            up = upsert_mesclados(conn, schema, table_mesclados, list(rows_map.values()))
            de = delete_from_other_tables(conn, do_delete, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, del_ids)
            return up, de

        upserted, deleted = commit_with_retry(conn, _do_write, log, max_retries=write_retries)
        log.info("done merges_upserted=%d deleted_from_base=%d", upserted, deleted)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if sleep_after_run > 0:
        time.sleep(sleep_after_run)


if __name__ == "__main__":
    main()
