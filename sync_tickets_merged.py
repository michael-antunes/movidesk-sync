import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_top_window_by_max_id_v1_2026-01-27"

MovideskResponse = Union[Dict[str, Any], List[Any], None]


# -------------------------
# Env helpers
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


def env_bool(k: str, default: bool = False) -> bool:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on", "sim")


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


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


# -------------------------
# DB helpers
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


def get_max_updated_at(conn, schema: str, table: str, col: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(f"select max({qident(col)}) from {qname(schema, table)}")
        v = cur.fetchone()[0]
    return parse_dt(v)


def get_max_id(conn, schema: str, table: str, col: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(f"select max({qident(col)}) from {qname(schema, table)}")
        v = cur.fetchone()[0]
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]):
    if not rows:
        return 0

    sql = f"""
    insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update
      set merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {qname(schema, table)}.merged_at),
          raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# -------------------------
# Movidesk API (/tickets/merged)
# -------------------------
def movidesk_get(
    sess: requests.Session,
    base_url: str,
    token: str,
    params: Dict[str, Any],
    timeout: int,
) -> Tuple[MovideskResponse, Optional[int], str]:
    """Retorna (json|None, status, text_snippet)"""
    url = f"{base_url.rstrip('/')}/tickets/merged"
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


def extract_relations_from_obj(
    obj: Dict[str, Any], fallback_queried_id: int
) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    """
    Normaliza 1 objeto em relações (source_ticket -> dest_ticket).
    """
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

    # Caso 1: ticketId(destino) + mergedTicketId(origem)
    if obj.get("ticketId") is not None and (obj.get("mergedTicketId") is not None or obj.get("mergedTicketID") is not None):
        try:
            dest = int(obj.get("ticketId"))
            src = int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
            out.append((src, dest, merged_at, payload))
            return out
        except Exception:
            pass

    # Caso 2: consulta pelo "filho" retorna mergedIntoId (destino)
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

    # Caso 3: lista de filhos mergedTicketsIds (consulta pelo destino)
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

    # Dedup por ticket_id (source)
    dedup: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
    for (src, dest, merged_at, payload) in out:
        dedup[int(src)] = (int(src), int(dest), merged_at, payload)
    return list(dedup.values())


def fetch_merged_relations(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
    query_keys: List[str],
) -> Tuple[List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]], Optional[int], Optional[str], int]:
    """
    Tenta chaves na ordem (ticketId -> id -> q).
    Retorna: (rows, status, key_usada, tries_count)
    """
    last_status: Optional[int] = None
    tries = 0
    for key in query_keys:
        tries += 1
        params = {key: str(ticket_id)}
        data, st, txt = movidesk_get(sess, base_url, token, params, timeout)
        last_status = st

        # 404: tenta próximo key
        if st == 404:
            continue

        # 200 (ou outro): decide aqui
        rows = extract_relations(data, ticket_id)

        # se 200, já vale (mesmo vazio)
        if st == 200:
            return rows, st, key, tries

        # outros erros: devolve pra log e para
        return rows, st, key, tries

    return [], last_status, None, tries


# -------------------------
# Main
# -------------------------
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
    source_date_col = env_str("SOURCE_DATE_COL", "updated_at")
    source_id_col = env_str("SOURCE_ID_COL", "id")

    window_size = env_int("WINDOW_SIZE", 50)
    rpm = env_float("RPM", 10.0)
    pause_seconds = env_int("PAUSE_SECONDS", 0)
    http_timeout = env_int("HTTP_TIMEOUT", 45)

    lock_timeout_ms = env_int("PG_LOCK_TIMEOUT_MS", 5000)
    lock_retries = env_int("PG_LOCK_RETRIES", 6)

    query_keys = [k.strip() for k in env_str("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if k.strip()]

    delay_between_requests = (60.0 / rpm) if rpm and rpm > 0 else 0.0

    log.info(
        "script_version=%s window_size=%d rpm=%.2f pause_seconds=%d query_keys=%s",
        SCRIPT_VERSION,
        window_size,
        rpm,
        pause_seconds,
        query_keys,
    )

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    set_session_timeouts(conn, lock_timeout_ms)

    ensure_table(conn, db_schema, table_name)
    conn.commit()

    anchor_ts = get_max_updated_at(conn, source_schema, source_table, source_date_col)
    max_id = get_max_id(conn, source_schema, source_table, source_id_col)

    log.info(
        "anchor_ts_db_max=%s (%s.%s.%s) max_id_db=%s (%s.%s.%s)",
        anchor_ts.isoformat() if anchor_ts else None,
        source_schema, source_table, source_date_col,
        max_id,
        source_schema, source_table, source_id_col,
    )

    if not max_id or max_id <= 0:
        log.warning("max_id_db veio vazio/0. Abortando.")
        return

    start_id = int(max_id)
    end_id = max(1, start_id - window_size + 1)
    ids = list(range(start_id, end_id - 1, -1))

    sess = requests.Session()

    total_checked = 0
    total_upsert = 0
    total_rows_found = 0

    # acumula em batch pra reduzir commits
    batch: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []

    for idx, tid in enumerate(ids, start=1):
        total_checked += 1

        log.info("validando_ticket %d/%d ticket_id=%d", idx, len(ids), tid)

        rows, st, key_used, tries = fetch_merged_relations(
            sess=sess,
            base_url=base_url,
            token=token,
            ticket_id=tid,
            timeout=http_timeout,
            query_keys=query_keys,
        )

        total_rows_found += len(rows)

        log.info(
            "resultado_ticket ticket_id=%d status=%s key=%s tries=%d rows=%d",
            tid,
            st,
            key_used,
            tries,
            len(rows),
        )

        # log detalhado de cada relação encontrada (pra você bater o olho)
        for (src, dest, merged_at, _payload) in rows:
            log.info(
                "merge_encontrada source_ticket=%d merged_into=%d merged_at=%s (query_ticket=%d)",
                src,
                dest,
                merged_at.isoformat() if merged_at else None,
                tid,
            )

        # acumula batch
        if rows:
            batch.extend(rows)

        # flush a cada 200 relações (ou no fim)
        if len(batch) >= 200:
            def do_upsert():
                return upsert_rows(conn, db_schema, table_name, batch)

            n = commit_with_retry(conn, do_upsert, log=log, max_retries=lock_retries)
            total_upsert += n
            batch.clear()

        if delay_between_requests > 0:
            time.sleep(delay_between_requests)

    # flush final
    if batch:
        def do_upsert_final():
            return upsert_rows(conn, db_schema, table_name, batch)

        n = commit_with_retry(conn, do_upsert_final, log=log, max_retries=lock_retries)
        total_upsert += n
        batch.clear()

    log.info(
        "fim checked=%d total_rows_found=%d upsert=%d window=[%d..%d]",
        total_checked,
        total_rows_found,
        total_upsert,
        start_id,
        end_id,
    )

    if pause_seconds > 0:
        log.info("pausando %ds antes de finalizar...", pause_seconds)
        time.sleep(pause_seconds)

    conn.close()


if __name__ == "__main__":
    main()
