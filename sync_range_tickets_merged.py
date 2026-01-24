import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


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


def pg_connect():
    dsn = env_str("NEON_DSN") or env_str("DATABASE_URL")
    if not dsn:
        raise RuntimeError("NEON_DSN não definido")
    return psycopg2.connect(dsn)


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
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


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


def ensure_table(conn, schema: str, table: str):
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
        cur.execute(
            f"""
            do $$
            begin
              if exists (
                select 1
                from information_schema.columns
                where table_schema='{schema}'
                  and table_name='{table}'
                  and column_name='raw_paylcad'
              ) then
                execute 'alter table {qname(schema, table)} rename column raw_paylcad to raw_payload';
              end if;
            end $$;
            """
        )
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_into_id bigint")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_at timestamptz")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists raw_payload jsonb")
        cur.execute(f"delete from {qname(schema, table)} where merged_into_id is null")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists situacao_mesclado")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_tickets")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_tickets_ids")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists merged_ticket_ids_arr")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists last_update")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists synced_at")
        cur.execute(f"alter table {qname(schema, table)} drop column if exists updated_at")
        cur.execute(
            f"""
            do $$
            begin
              if not exists (
                select 1
                from pg_constraint
                where conrelid='{schema}.{table}'::regclass
                  and contype='p'
              ) then
                execute 'alter table {qname(schema, table)} add primary key (ticket_id)';
              end if;
            end $$;
            """
        )


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}
    last_status = None
    for i in range(5):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code
            if r.status_code == 404:
                return None, 404
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1))
                continue
            if r.status_code != 200:
                return None, r.status_code
            data = r.json()
            return (data if isinstance(data, dict) else None), 200
        except Exception:
            time.sleep(2 * (i + 1))
    return None, last_status


def read_control(conn, schema: str, control_table: str) -> Tuple[str, int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select ctid::text, id_inicial::bigint, id_final::bigint, id_atual_merged::bigint
            from {qname(schema, control_table)}
            order by data_fim desc nulls last, data_inicio desc nulls last, id_inicial desc nulls last
            limit 1
            """
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError("range_scan_control vazio")
        ctid = str(r[0])
        id_inicial = int(r[1])
        id_final = int(r[2])
        id_ptr = int(r[3]) if r[3] is not None else id_inicial
        if id_ptr < id_inicial:
            id_ptr = id_inicial
        return ctid, id_inicial, id_final, id_ptr


def update_ptr(conn, schema: str, control_table: str, ctid_text: str, new_ptr: int):
    with conn.cursor() as cur:
        cur.execute(f"update {qname(schema, control_table)} set id_atual_merged=%s where ctid=%s::tid", (new_ptr, ctid_text))


def build_batch(id_ptr: int, id_final: int, limit: int) -> List[int]:
    if id_ptr <= id_final:
        return []
    stop = max(id_final, id_ptr - limit + 1)
    return list(range(id_ptr, stop - 1, -1))


def extract_merge_rows(queried_id: int, raw: Dict[str, Any]) -> List[Tuple[int, int, Optional[datetime], Any]]:
    merged_at = parse_dt(raw.get("mergedDate") or raw.get("performedAt") or raw.get("date") or raw.get("lastUpdate") or raw.get("last_update") or raw.get("mergedAt"))
    payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))

    merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
    if not merged_ids:
        mt = raw.get("mergedTickets") or raw.get("mergedTicketsList")
        if isinstance(mt, list):
            tmp = []
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

    rows: List[Tuple[int, int, Optional[datetime], Any]] = []
    if merged_ids:
        for mid in merged_ids:
            mid_i = int(mid)
            rows.append((mid_i, int(queried_id), merged_at, payload))
        return rows

    merged_into = raw.get("mergedIntoId") or raw.get("mergedIntoTicketId") or raw.get("mainTicketId") or raw.get("mainTicketID") or raw.get("principalTicketId") or raw.get("principalId") or raw.get("mergedInto")
    if merged_into is not None:
        try:
            rows.append((int(queried_id), int(merged_into), merged_at, payload))
        except Exception:
            pass
    return rows


def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Any]]) -> int:
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


def delete_from_other_tables(conn, resolvidos_schema: str, resolvidos_table: str, abertos_schema: str, abertos_table: str, ids: List[int]):
    if not ids:
        return
    uniq = sorted(set(int(x) for x in ids))
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where ticket_id = any(%s)", (uniq,))
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where ticket_id = any(%s)", (uniq,))


def main():
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("range-merged")

    token = env_str("MOVIDESK_TOKEN")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    http_timeout = env_int("HTTP_TIMEOUT", 60)

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table_mesclados = env_str("TABLE_NAME", "tickets_mesclados")
    control_table = env_str("CONTROL_TABLE", "range_scan_control")

    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos")
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail")

    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos")

    limit = env_int("LIMIT", 80)
    rpm = env_float("RPM", 9.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    dry_run = env_bool("DRY_RUN", False)
    commit_every = env_int("COMMIT_EVERY", 10)
    max_runtime_sec = env_int("MAX_RUNTIME_SEC", 1100)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = pg_connect()
    conn.autocommit = False

    try:
        ensure_table(conn, schema, table_mesclados)
        ctid_text, id_inicial, id_final, id_ptr = read_control(conn, schema, control_table)
        conn.commit()

        deadline = time.monotonic() + max(60, max_runtime_sec)
        total_checked = 0
        total_rel = 0
        total_upserted = 0
        total_deleted = 0
        last_status_counts: Dict[int, int] = {}
        id_ptr_start = id_ptr

        while id_ptr > id_final and time.monotonic() < deadline:
            batch = build_batch(id_ptr, id_final, limit)
            if not batch:
                break

            rows_map: Dict[int, Tuple[int, int, Optional[datetime], Any]] = {}
            del_ids: List[int] = []
            checked = 0
            rel = 0
            last_processed = None

            for ticket_id in batch:
                if time.monotonic() >= deadline:
                    break

                checked += 1
                last_processed = int(ticket_id)

                raw, st = movidesk_get_merged(sess, base_url, token, int(ticket_id), http_timeout)
                if st is not None:
                    last_status_counts[st] = last_status_counts.get(st, 0) + 1

                if raw:
                    rows = extract_merge_rows(int(ticket_id), raw)
                    if rows:
                        rel += len(rows)
                        for t_id, merged_into_id, merged_at, payload in rows:
                            rows_map[int(t_id)] = (int(t_id), int(merged_into_id), merged_at, payload)
                            del_ids.append(int(t_id))

                if throttle > 0:
                    time.sleep(throttle)

                if checked % max(1, commit_every) == 0:
                    new_ptr = int(ticket_id) - 1
                    if new_ptr < id_final:
                        new_ptr = id_final

                    if dry_run:
                        log.info("partial checked=%d rel=%d upsert=%d id_ptr=%s->%s", checked, rel, len(rows_map), id_ptr, new_ptr)
                    else:
                        ensure_table(conn, schema, table_mesclados)
                        upserted = upsert_mesclados(conn, schema, table_mesclados, list(rows_map.values()))
                        delete_from_other_tables(conn, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, del_ids)
                        update_ptr(conn, schema, control_table, ctid_text, new_ptr)
                        conn.commit()
                        total_upserted += upserted
                        total_deleted += len(set(del_ids))
                        total_rel += rel
                        total_checked += checked

                    rows_map.clear()
                    del_ids.clear()

            if last_processed is None:
                break

            new_ptr = int(last_processed) - 1
            if new_ptr < id_final:
                new_ptr = id_final

            if dry_run:
                log.info("batch checked=%d rel=%d upsert=%d id_ptr=%s->%s", checked, rel, len(rows_map), id_ptr, new_ptr)
                id_ptr = new_ptr
                continue

            ensure_table(conn, schema, table_mesclados)
            upserted = upsert_mesclados(conn, schema, table_mesclados, list(rows_map.values()))
            delete_from_other_tables(conn, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, del_ids)
            update_ptr(conn, schema, control_table, ctid_text, new_ptr)
            conn.commit()

            total_upserted += upserted
            total_deleted += len(set(del_ids))
            total_rel += rel
            total_checked += checked

            id_ptr = new_ptr
            log.info("progress id_ptr=%s", id_ptr)

        if dry_run:
            log.info("done checked=%d rel=%d id_ptr=%s->%s", total_checked, total_rel, id_ptr_start, id_ptr)
        else:
            log.info("done checked=%d rel=%d upsert=%d deleted=%d id_ptr=%s->%s statuses=%s", total_checked, total_rel, total_upserted, total_deleted, id_ptr_start, id_ptr, json.dumps(last_status_counts, ensure_ascii=False))

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
