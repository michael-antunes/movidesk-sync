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
    except ValueError:
        return default


def env_float(k: str, default: float) -> float:
    v = env_str(k)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def env_bool(k: str, default: bool) -> bool:
    v = env_str(k)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "t", "yes", "y", "sim"):
        return True
    if v in ("0", "false", "f", "no", "n", "nao", "não"):
        return False
    return default


def pg_connect():
    dsn = env_str("NEON_DSN") or env_str("DATABASE_URL")
    if not dsn:
        raise RuntimeError("NEON_DSN não definido")
    return psycopg2.connect(dsn)


def ensure_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id INT PRIMARY KEY,
                ticket_id INT,
                merged_ticket_id INT,
                created_date TIMESTAMPTZ,
                created_by_id INT,
                created_by_name TEXT,
                last_update TIMESTAMPTZ,
                updated_at_utc TIMESTAMPTZ NOT NULL,
                raw JSONB
            );
            """
        )


def ensure_control_table(conn, schema: str, control_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{control_table} (
                id INT PRIMARY KEY,
                id_inicial INT,
                id_final INT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            f"""
            INSERT INTO {schema}.{control_table} (id, id_inicial, id_final)
            VALUES (0, 0, 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )


def read_control(conn, schema: str, control_table: str) -> Tuple[int, int, int, int]:
    """
    Retorna:
      id_inicial, id_final, last_processed_db, next_id
    """
    ensure_control_table(conn, schema, control_table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, id_inicial, id_final FROM {schema}.{control_table} WHERE id=0;"
        )
        row = cur.fetchone()
        if not row:
            return 0, 0, 0, 0

        last_processed_db = int(row[0] or 0)
        id_inicial = int(row[1] or 0)
        id_final = int(row[2] or 0)

        # Regra do range scan: começa do last_processed (ou id_inicial se last=0)
        next_id = last_processed_db if last_processed_db > 0 else id_inicial
        return id_inicial, id_final, last_processed_db, next_id


def update_last_processed(conn, schema: str, control_table: str, new_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {schema}.{control_table}
               SET id = %s,
                   updated_at = NOW()
             WHERE id=0;
            """,
            (new_id,),
        )


def movidesk_get_merged(
    sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """Consulta /tickets/merged.

    A doc do Movidesk usa o parâmetro `ticketId`, mas em alguns cenários aparece `id`.
    Para garantir compatibilidade, tentamos os dois (igual ao script que já funciona).
    """
    url = f"{base_url.rstrip('/')}/tickets/merged"

    for key in ("ticketId", "id"):
        params = {"token": token, key: str(ticket_id)}
        last_status = None

        for i in range(5):
            try:
                r = sess.get(url, params=params, timeout=timeout)
                last_status = r.status_code

                if r.status_code == 404:
                    # tenta o próximo parâmetro
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

    return None, 404


def normalize_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def normalize_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def normalize_dt(x: Any) -> Optional[datetime]:
    if not x:
        return None
    if isinstance(x, datetime):
        return x
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None


def upsert_merged(conn, schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    cols = [
        "id",
        "ticket_id",
        "merged_ticket_id",
        "created_date",
        "created_by_id",
        "created_by_name",
        "last_update",
        "updated_at_utc",
        "raw",
    ]

    values = []
    for r in rows:
        values.append(
            (
                normalize_int(r.get("id")),
                normalize_int(r.get("ticketId")),
                normalize_int(r.get("mergedTicketId")),
                normalize_dt(r.get("createdDate")),
                normalize_int((r.get("createdBy") or {}).get("id")),
                normalize_str((r.get("createdBy") or {}).get("name")),
                normalize_dt(r.get("lastUpdate")),
                datetime.now(timezone.utc),
                json.dumps(r, ensure_ascii=False),
            )
        )

    with conn.cursor() as cur:
        insert_sql = f"""
            INSERT INTO {schema}.{table} ({",".join(cols)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                ticket_id = EXCLUDED.ticket_id,
                merged_ticket_id = EXCLUDED.merged_ticket_id,
                created_date = EXCLUDED.created_date,
                created_by_id = EXCLUDED.created_by_id,
                created_by_name = EXCLUDED.created_by_name,
                last_update = EXCLUDED.last_update,
                updated_at_utc = EXCLUDED.updated_at_utc,
                raw = EXCLUDED.raw;
        """
        psycopg2.extras.execute_values(cur, insert_sql, values, page_size=200)
        return len(values)


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
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {resolvidos_schema}.{resolvidos_table} WHERE id = ANY(%s);",
            (ids,),
        )
        cur.execute(
            f"DELETE FROM {abertos_schema}.{abertos_table} WHERE id = ANY(%s);",
            (ids,),
        )
        return len(ids)


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

    limit = env_int("LIMIT", 10)
    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    dry_run = env_bool("DRY_RUN", False)
    max_runtime_sec = env_int("MAX_RUNTIME_SEC", 1100)
    commit_every = env_int("COMMIT_EVERY", 10)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = pg_connect()
    conn.autocommit = False

    try:
        ensure_table(conn, schema, table_mesclados)
        id_inicial, id_final, last_processed_db, next_id = read_control(conn, schema, control_table)
        conn.commit()

        log.info("begin id_inicial=%s id_final=%s last_processed=%s next_id=%s", id_inicial, id_final, last_processed_db, next_id)

        if next_id < id_final:
            log.info("done id_inicial=%s id_final=%s last_processed=%s", id_inicial, id_final, last_processed_db)
            return

        deadline = time.monotonic() + max(60, max_runtime_sec)
        status_counts: Dict[int, int] = {}
        total_checked = 0
        total_rel = 0
        total_upserted = 0
        total_deleted = 0

        buffer_rows: List[Dict[str, Any]] = []
        buffer_del_ids: List[int] = []
        buffer_count = 0

        while next_id >= id_final and time.monotonic() < deadline and total_checked < limit:
            t0 = time.monotonic()

            data, status = movidesk_get_merged(sess, base_url, token, next_id, http_timeout)
            status_counts[int(status or 0)] = status_counts.get(int(status or 0), 0) + 1
            total_checked += 1

            if status == 200 and isinstance(data, dict) and data:
                if any(k in data for k in ("id", "ticketId", "mergedTicketId")):
                    total_rel += 1
                    buffer_rows.append(data)

                    # remove os tickets envolvidos das outras tabelas
                    tid = normalize_int(data.get("ticketId"))
                    mid = normalize_int(data.get("mergedTicketId"))
                    if tid is not None:
                        buffer_del_ids.append(tid)
                    if mid is not None:
                        buffer_del_ids.append(mid)

            buffer_count += 1

            # commit em lote
            if buffer_count >= commit_every:
                if not dry_run:
                    if buffer_rows:
                        total_upserted += upsert_merged(conn, schema, table_mesclados, buffer_rows)
                    if buffer_del_ids:
                        total_deleted += delete_from_other_tables(
                            conn,
                            resolvidos_schema,
                            resolvidos_table,
                            abertos_schema,
                            abertos_table,
                            list(set(buffer_del_ids)),
                        )
                    update_last_processed(conn, schema, control_table, next_id)
                    conn.commit()

                buffer_rows.clear()
                buffer_del_ids.clear()
                buffer_count = 0

            next_id -= 1

            # throttle por RPM
            if throttle > 0:
                dt = time.monotonic() - t0
                sleep_s = throttle - dt
                if sleep_s > 0:
                    time.sleep(sleep_s)

        # flush final
        if buffer_count > 0 and not dry_run:
            if buffer_rows:
                total_upserted += upsert_merged(conn, schema, table_mesclados, buffer_rows)
            if buffer_del_ids:
                total_deleted += delete_from_other_tables(
                    conn,
                    resolvidos_schema,
                    resolvidos_table,
                    abertos_schema,
                    abertos_table,
                    list(set(buffer_del_ids)),
                )
            update_last_processed(conn, schema, control_table, next_id + 1)  # último processado
            conn.commit()

        log.info(
            "end checked=%d rel=%d upserted=%d deleted=%d next_id=%s statuses=%s",
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
