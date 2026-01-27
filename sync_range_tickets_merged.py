import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import psycopg2
import psycopg2.extras


# -------------------------
# Helpers (env / quoting)
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
# DB setup / retry logic
# -------------------------

def set_session_timeouts(conn, lock_timeout_ms: int) -> None:
    """Define lock_timeout na sessão pra não travar indefinidamente."""
    if lock_timeout_ms <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(f"set lock_timeout = '{int(lock_timeout_ms)}ms'")


def commit_with_retry(conn, fn, log: logging.Logger, max_retries: int = 6):
    """
    Executa fn() + commit.
    Se bater lock timeout/deadlock, faz rollback e retry com backoff.
    """
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
    raise RuntimeError("Falhou por lock/deadlock muitas vezes. Tente novamente quando o DB estiver livre.")


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
        # índices úteis
        cur.execute(f"create index if not exists ix_{table}_merged_into_id on {qname(schema, table)} (merged_into_id)")
        cur.execute(f"create index if not exists ix_{table}_merged_at on {qname(schema, table)} (merged_at)")


# -------------------------
# Control (range_scan_control)
# -------------------------

def read_control(conn, schema: str, control_table: str) -> Tuple[int, int, Optional[int], int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select id_inicial::bigint, id_final::bigint, id_atual_merged::bigint
            from {qname(schema, control_table)}
            order by data_fim desc nulls last, data_inicio desc nulls last, id_inicial desc nulls last
            limit 1
            """
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError("range_scan_control está vazio.")
        id_inicial = int(r[0])
        id_final = int(r[1])
        last_processed = int(r[2]) if r[2] is not None else None

        # Regra do seu loop: next_id = id_atual_merged - 1
        next_id = id_inicial if last_processed is None else (last_processed - 1)
        if next_id > id_inicial:
            next_id = id_inicial

        return id_inicial, id_final, last_processed, next_id


def update_last_processed(conn, schema: str, control_table: str, last_processed: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {qname(schema, control_table)}
               set id_atual_merged = %s
             where ctid = (
               select ctid
                 from {qname(schema, control_table)}
                order by data_fim desc nulls last,
                         data_inicio desc nulls last,
                         id_inicial desc nulls last
                limit 1
             )
            """,
            (int(last_processed),),
        )
        if cur.rowcount == 0:
            raise RuntimeError("Não consegui atualizar id_atual_merged (0 rows).")


def build_batch(next_id: int, id_final: int, limit: int) -> List[int]:
    if next_id < id_final:
        return []
    stop = max(id_final, next_id - limit + 1)
    return list(range(next_id, stop - 1, -1))


# -------------------------
# Movidesk API (/tickets/merged)
# -------------------------

MovideskMergedResponse = Union[Dict[str, Any], List[Any]]


def movidesk_get_merged(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
) -> Tuple[Optional[MovideskMergedResponse], Optional[int]]:
    """
    Consulta o endpoint /tickets/merged.

    IMPORTANTÍSSIMO:
      - o parâmetro "oficial" é ticketId
      - alguns ambientes aceitam id (mas nem sempre significa id do ticket)

    A resposta pode vir como dict OU como list.
    """
    url = f"{base_url.rstrip('/')}/tickets/merged"

    for key in ("ticketId", "id"):
        params = {"token": token, key: str(ticket_id)}
        last_status: Optional[int] = None

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

                data = r.json()  # pode ser list ou dict
                return data, 200

            except Exception:
                time.sleep(2 * (i + 1))

        if last_status == 404:
            continue

        return None, last_status

    return None, 404


# -------------------------
# Parse de "merged" (várias variações)
# -------------------------

def extract_rows_from_item(
    queried_id: int,
    item: Dict[str, Any],
) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    """
    Converte um item retornado pela API em linhas (ticket_id, merged_into_id, merged_at, raw_payload).

    Casos cobertos:
      A) {ticketId: <principal>, mergedTicketId: <mesclado>}
      B) {ticketId: <mesclado>, mergedIntoTicketId: <principal>} (ou mergedIntoId)
      C) payload com mergedTicketsIds (lista) dentro do ticket principal
      D) payload com mergedIntoId dentro do ticket mesclado
    """
    merged_at = parse_dt(
        item.get("mergedDate")
        or item.get("mergedAt")
        or item.get("performedAt")
        or item.get("date")
        or item.get("createdAt")
        or item.get("createdDate")
        or item.get("lastUpdate")
        or item.get("last_update")
    )
    payload = json_payload(item)

    rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []

    # A) principal + mesclado
    if item.get("ticketId") is not None and item.get("mergedTicketId") is not None:
        try:
            principal = int(item["ticketId"])
            merged_ticket = int(item["mergedTicketId"])
            rows.append((merged_ticket, principal, merged_at, payload))
            return rows
        except Exception:
            pass

    # B) mesclado -> principal
    if item.get("ticketId") is not None and (item.get("mergedIntoTicketId") is not None or item.get("mergedIntoId") is not None):
        try:
            merged_ticket = int(item["ticketId"])
            principal = int(item.get("mergedIntoTicketId") or item.get("mergedIntoId"))
            rows.append((merged_ticket, principal, merged_at, payload))
            return rows
        except Exception:
            pass

    # C) lista de mesclados dentro do principal
    merged_ids = to_int_list(item.get("mergedTicketsIds") or item.get("mergedTicketsIDs") or item.get("mergedTicketsIdsList"))
    if not merged_ids:
        mt = item.get("mergedTickets") or item.get("mergedTicketsList")
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
        principal_guess = item.get("ticketId") or item.get("principalTicketId") or item.get("principalId") or item.get("mainTicketId") or queried_id
        try:
            principal = int(principal_guess)
        except Exception:
            principal = int(queried_id)

        for mid in merged_ids:
            rows.append((int(mid), principal, merged_at, payload))
        return rows

    # D) este ticket foi mesclado em outro
    merged_into = (
        item.get("mergedIntoId")
        or item.get("mergedIntoTicketId")
        or item.get("mainTicketId")
        or item.get("principalTicketId")
        or item.get("principalId")
        or item.get("mergedInto")
    )
    if merged_into is not None:
        try:
            rows.append((int(queried_id), int(merged_into), merged_at, payload))
        except Exception:
            pass

    return rows


def extract_merge_rows_any(
    queried_id: int,
    raw_any: MovideskMergedResponse,
) -> List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]]:
    """
    Aceita resposta da API como dict OU list.
    Retorna lista de linhas.
    """
    if raw_any is None:
        return []

    if isinstance(raw_any, list):
        out: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = []
        for it in raw_any:
            if isinstance(it, dict):
                out.extend(extract_rows_from_item(queried_id, it))
        return out

    if isinstance(raw_any, dict):
        return extract_rows_from_item(queried_id, raw_any)

    return []


# -------------------------
# Writes
# -------------------------

def upsert_mesclados(
    conn,
    schema: str,
    table: str,
    rows: List[Tuple[int, int, Optional[datetime], psycopg2.extras.Json]],
) -> int:
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
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    ids: List[int],
) -> int:
    if not ids:
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
    log = logging.getLogger("sync_range_tickets_merged")

    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN") or env_str("DATABASE_URL")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1") or "https://api.movidesk.com/public/v1"

    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não informado")
    if not dsn:
        raise RuntimeError("NEON_DSN não informado")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    table_mesclados = env_str("TABLE_NAME", "tickets_mesclados") or "tickets_mesclados"
    control_table = env_str("CONTROL_TABLE", "range_scan_control") or "range_scan_control"

    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail") or "tickets_resolvidos_detail"
    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual") or "visualizacao_atual"
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos") or "tickets_abertos"

    limit = env_int("LIMIT", 10)
    rpm = env_float("RPM", 10.0)
    dry_run = env_bool("DRY_RUN", False)
    max_runtime_sec = env_int("MAX_RUNTIME_SEC", 1100)
    http_timeout = env_int("HTTP_TIMEOUT", 60)

    lock_timeout_ms = env_int("LOCK_TIMEOUT_MS", 5000)  # resolve teu erro do log
    write_retries = env_int("WRITE_RETRIES", 6)

    debug_ids = set(to_int_list(env_str("DEBUG_TICKET_IDS") or ""))

    throttle = 0.0
    if rpm > 0:
        throttle = 60.0 / float(rpm)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        set_session_timeouts(conn, lock_timeout_ms)
        ensure_table(conn, schema, table_mesclados)
        conn.commit()

        id_inicial, id_final, last_processed_db, next_id = read_control(conn, schema, control_table)
        conn.commit()

        log.info("begin id_inicial=%s id_final=%s last_processed=%s next_id=%s", id_inicial, id_final, last_processed_db, next_id)

        deadline = time.monotonic() + max(60, max_runtime_sec)

        status_counts: Dict[int, int] = {}
        total_checked = 0
        total_rel = 0
        total_upserted = 0
        total_deleted = 0

        while next_id >= id_final and time.monotonic() < deadline:
            batch = build_batch(next_id, id_final, limit)
            if not batch:
                break

            rows_map: Dict[int, Tuple[int, int, Optional[datetime], psycopg2.extras.Json]] = {}
            del_ids: List[int] = []

            checked = 0
            rel = 0
            last_processed_run: Optional[int] = None

            for ticket_id in batch:
                if time.monotonic() >= deadline:
                    break

                checked += 1
                last_processed_run = int(ticket_id)

                raw_any, st = movidesk_get_merged(sess, base_url, token, int(ticket_id), http_timeout)
                if st is not None:
                    status_counts[st] = status_counts.get(st, 0) + 1

                if ticket_id in debug_ids:
                    t = type(raw_any).__name__ if raw_any is not None else "None"
                    log.info("DEBUG ticket_id=%s status=%s type=%s raw=%s", ticket_id, st, t, json.dumps(raw_any, ensure_ascii=False)[:4000])

                if raw_any is not None:
                    rows = extract_merge_rows_any(int(ticket_id), raw_any)
                    if rows:
                        rel += len(rows)
                        for t_id, merged_into_id, merged_at, payload in rows:
                            # 1 registro por ticket_id (upsert)
                            rows_map[int(t_id)] = (int(t_id), int(merged_into_id), merged_at, payload)
                            del_ids.append(int(t_id))

                if throttle > 0:
                    time.sleep(throttle)

            if last_processed_run is None:
                break

            if dry_run:
                next_id = last_processed_run - 1
                total_checked += checked
                total_rel += rel
                log.info("progress next_id=%s last_processed=%s checked=%d upsert=%d deleted=%d", next_id, last_processed_run, checked, 0, 0)
                continue

            def _do_write():
                upserted = upsert_mesclados(conn, schema, table_mesclados, list(rows_map.values()))
                deleted = delete_from_other_tables(conn, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, del_ids)
                update_last_processed(conn, schema, control_table, int(last_processed_run))
                return upserted, deleted

            upserted, deleted = commit_with_retry(conn, _do_write, log, max_retries=write_retries)

            next_id = last_processed_run - 1

            total_checked += checked
            total_rel += rel
            total_upserted += upserted
            total_deleted += deleted

            log.info("progress next_id=%s last_processed=%s checked=%d upsert=%d deleted=%d", next_id, last_processed_run, checked, upserted, deleted)

        log.info(
            "end checked=%d rel=%d upsert=%d deleted=%d next_id=%s statuses=%s",
            total_checked,
            total_rel,
            total_upserted,
            total_deleted,
            next_id,
            json.dumps(status_counts, ensure_ascii=False),
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
