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
    v = os.getenv(k)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def env_bool(k: str, default: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


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
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_into_id bigint")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists merged_at timestamptz")
        cur.execute(f"alter table {qname(schema, table)} add column if not exists raw_payload jsonb")
        cur.execute(f"create index if not exists ix_tickets_mesclados_merged_into on {qname(schema, table)} (merged_into_id)")


def movidesk_get_merged(
    sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    # A documentação do endpoint /tickets/merged usa o parâmetro `ticketId`, mas
    # há cenários em que `id` também aparece. Pra não dar dor de cabeça,
    # tentamos os dois.
    for key in ("ticketId", "id"):
        params = {"token": token, key: str(ticket_id)}
        last_status = None
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
                data = r.json()
                return (data if isinstance(data, dict) else None), 200
            except Exception:
                time.sleep(2 * (i + 1))
        if last_status == 404:
            continue
        return None, last_status
    return None, 404


def parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    # tenta ISO-8601 mais comum
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        return None


def extract_merge_rows(ticket_id: int, raw: Dict[str, Any]) -> List[Tuple[int, int, Optional[datetime], Any]]:
    rows: List[Tuple[int, int, Optional[datetime], Any]] = []

    merged_at = parse_dt(raw.get("mergedAt") or raw.get("merged_at") or raw.get("mergedDate") or raw.get("merged_date"))
    payload = raw

    merged_tickets_ids = raw.get("mergedTicketsIds") or raw.get("mergedTickets") or raw.get("merged_tickets_ids")
    merged_into_id = raw.get("mergedIntoId") or raw.get("merged_into_id") or raw.get("mergedInto") or raw.get("merged_into")

    # Caso A: ticket "principal" retornando lista de IDs mesclados
    ids = to_int_list(merged_tickets_ids)
    if ids:
        for mid in ids:
            rows.append((int(mid), int(ticket_id), merged_at, payload))
        return rows

    # Caso B: ticket mesclado retornando em qual ticket ele foi mesclado
    if merged_into_id is not None:
        try:
            rows.append((int(ticket_id), int(merged_into_id), merged_at, payload))
        except Exception:
            pass
    return rows


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
            raise RuntimeError("range_scan_control vazio")
        id_inicial = int(r[0])
        id_final = int(r[1])
        last_processed = int(r[2]) if r[2] is not None else None
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


def build_batch(next_id: int, id_final: int, limit: int) -> List[int]:
    if next_id < id_final:
        return []
    end_id = max(id_final, next_id - limit + 1)
    return list(range(next_id, end_id - 1, -1))


def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
            values %s
            on conflict (ticket_id) do update
              set merged_into_id = excluded.merged_into_id,
                  merged_at = excluded.merged_at,
                  raw_payload = excluded.raw_payload
            """,
            rows,
            template="(%s, %s, %s, %s)",
            page_size=1000,
        )
    return len(rows)


def delete_from_other_tables(
    conn,
    resolvidos_schema: str,
    resolvidos_table: str,
    abertos_schema: str,
    abertos_table: str,
    ticket_ids: List[int],
) -> int:
    if not ticket_ids:
        return 0
    ids = list({int(x) for x in ticket_ids if x is not None})
    if not ids:
        return 0
    deleted = 0
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(resolvidos_schema, resolvidos_table)} where id = any(%s)", (ids,))
        deleted += cur.rowcount
        cur.execute(f"delete from {qname(abertos_schema, abertos_table)} where id = any(%s)", (ids,))
        deleted += cur.rowcount
    return deleted


def main() -> None:
    token = env_str("MOVIDESK_TOKEN")
    dsn = env_str("NEON_DSN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

    schema = env_str("DB_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    table_mesclados = env_str("TABLE_NAME", "tickets_mesclados") or "tickets_mesclados"
    control_table = env_str("CONTROL_TABLE", "range_scan_control") or "range_scan_control"

    resolvidos_schema = env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos"
    resolvidos_table = env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail") or "tickets_resolvidos_detail"

    abertos_schema = env_str("ABERTOS_SCHEMA", "visualizacao_atual") or "visualizacao_atual"
    abertos_table = env_str("ABERTOS_TABLE", "tickets_abertos") or "tickets_abertos"

    limit = env_int("LIMIT", 10)
    rpm = env_int("RPM", 10)
    dry_run = env_bool("DRY_RUN", False)
    max_runtime = env_int("MAX_RUNTIME_SEC", 1100)
    commit_every = env_int("COMMIT_EVERY", 10)
    http_timeout = env_int("HTTP_TIMEOUT", 60)
    log_level = env_str("LOG_LEVEL", "INFO") or "INFO"

    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync_range_tickets_merged")

    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não informado")
    if not dsn:
        raise RuntimeError("NEON_DSN não informado")

    throttle = 0.0
    if rpm > 0:
        throttle = 60.0 / float(rpm)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    deadline = time.monotonic() + float(max_runtime)

    status_counts: Dict[int, int] = {}

    total_checked = 0
    total_rel = 0
    total_upserted = 0
    total_deleted = 0

    try:
        id_inicial, id_final, last_processed_db, next_id = read_control(conn, schema, control_table)
        log.info("begin id_inicial=%s id_final=%s last_processed=%s next_id=%s", id_inicial, id_final, last_processed_db, next_id)

        sess = requests.Session()

        while next_id >= id_final and time.monotonic() < deadline:
            batch = build_batch(next_id, id_final, limit)
            if not batch:
                break

            rows_map: Dict[int, Tuple[int, int, Optional[datetime], Any]] = {}
            del_ids: List[int] = []

            checked = 0
            rel = 0
            last_processed_run: Optional[int] = None

            for ticket_id in batch:
                if time.monotonic() >= deadline:
                    break

                checked += 1
                last_processed_run = int(ticket_id)

                raw, st = movidesk_get_merged(sess, base_url, token, int(ticket_id), http_timeout)
                if st is not None:
                    status_counts[st] = status_counts.get(st, 0) + 1

                if raw:
                    rows = extract_merge_rows(int(ticket_id), raw)
                    if rows:
                        rel += len(rows)
                        for t_id, merged_into_id, merged_at, payload in rows:
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

            upserted = 0
            deleted = 0

            ensure_table(conn, schema, table_mesclados)
            upserted += upsert_mesclados(conn, schema, table_mesclados, list(rows_map.values()))
            deleted += delete_from_other_tables(conn, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, del_ids)
            update_last_processed(conn, schema, control_table, int(last_processed_run))
            conn.commit()

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
