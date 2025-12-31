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


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, principal_id: int, timeout: int) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(principal_id)}
    for i in range(5):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1))
                continue
            if r.status_code != 200:
                return None
            data = r.json()
            return data if isinstance(data, dict) else None
        except Exception:
            time.sleep(2 * (i + 1))
    return None


def read_control(conn, schema: str, control_table: str) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select id_inicial::bigint, id_final::bigint, id_atual_merged::bigint
            from {qname(schema, control_table)}
            limit 1
            """
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError("range_scan_control vazio")
        id_inicial = int(r[0])
        id_final = int(r[1])
        id_ptr = int(r[2]) if r[2] is not None else id_inicial
        return id_inicial, id_final, id_ptr


def update_ptr(conn, schema: str, control_table: str, new_ptr: int):
    with conn.cursor() as cur:
        cur.execute(f"update {qname(schema, control_table)} set id_atual_merged=%s", (new_ptr,))


def build_batch(id_ptr: int, id_final: int, limit: int) -> List[int]:
    if id_ptr <= id_final:
        return []
    stop = max(id_final, id_ptr - limit + 1)
    return list(range(id_ptr, stop - 1, -1))


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

    conn = pg_connect()
    conn.autocommit = False
    try:
        ensure_table(conn, schema, table_mesclados)
        _, id_final, id_ptr = read_control(conn, schema, control_table)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    batch = build_batch(id_ptr, id_final, limit)
    if not batch:
        return

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    rows_map: Dict[int, Tuple[int, int, Optional[datetime], Any]] = {}
    merged_found = 0
    checked = 0
    all_mesclados: List[int] = []

    for principal_id in batch:
        checked += 1
        raw = movidesk_get_merged(sess, base_url, token, principal_id, http_timeout)
        if raw:
            merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
            if merged_ids:
                merged_found += len(merged_ids)
                merged_at = parse_dt(raw.get("mergedDate") or raw.get("performedAt") or raw.get("date") or raw.get("lastUpdate") or raw.get("last_update"))
                payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))
                for mid in merged_ids:
                    mid_i = int(mid)
                    rows_map[mid_i] = (mid_i, int(principal_id), merged_at, payload)
                    all_mesclados.append(mid_i)
        if throttle > 0:
            time.sleep(throttle)

    rows = list(rows_map.values())

    new_ptr = batch[-1] - 1
    if new_ptr < id_final:
        new_ptr = id_final

    if dry_run:
        log.info("checked=%d merged_found=%d upsert=%d id_ptr=%s->%s", checked, merged_found, len(rows), id_ptr, new_ptr)
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        ensure_table(conn2, schema, table_mesclados)
        upserted = upsert_mesclados(conn2, schema, table_mesclados, rows)
        delete_from_other_tables(conn2, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, all_mesclados)
        update_ptr(conn2, schema, control_table, new_ptr)
        conn2.commit()
        log.info("checked=%d merged_found=%d upsert=%d id_ptr=%s->%s", checked, merged_found, upserted, id_ptr, new_ptr)
    except Exception:
        conn2.rollback()
        raise
    finally:
        conn2.close()


if __name__ == "__main__":
    main()
