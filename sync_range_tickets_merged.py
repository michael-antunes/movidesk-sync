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


def parse_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "t", "yes", "y", "sim"):
        return True
    if v in ("0", "false", "f", "no", "n", "nao", "não"):
        return False
    return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mk_logger() -> logging.Logger:
    log = logging.getLogger("sync_range_tickets_merged")
    if not log.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(fmt)
        log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log


log = mk_logger()


def pg_connect() -> psycopg2.extensions.connection:
    host = env_str("PGHOST")
    user = env_str("PGUSER")
    pwd = env_str("PGPASSWORD")
    db = env_str("PGDATABASE")
    port = env_int("PGPORT", 5432)

    if not all([host, user, pwd, db]):
        raise RuntimeError(
            "Variáveis de ambiente do Postgres ausentes: PGHOST/PGUSER/PGPASSWORD/PGDATABASE"
        )

    return psycopg2.connect(
        host=host, user=user, password=pwd, dbname=db, port=port, connect_timeout=15
    )


def ensure_control_table(conn, schema: str, control_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{control_table} (
                id INT PRIMARY KEY,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            f"""
            INSERT INTO {schema}.{control_table} (id)
            VALUES (0)
            ON CONFLICT (id) DO NOTHING;
            """
        )


def get_last_processed(conn, schema: str, control_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {schema}.{control_table} LIMIT 1;")
        row = cur.fetchone()
        return int(row[0]) if row else 0


def update_last_processed(conn, schema: str, control_table: str, new_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {schema}.{control_table}
               SET id = %s,
                   updated_at = NOW()
             WHERE TRUE;
            """,
            (new_id,),
        )


def movidesk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


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
        # Movidesk costuma vir em ISO 8601
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None


def upsert_merged(
    conn,
    schema: str,
    table: str,
    rows: List[Dict[str, Any]],
) -> int:
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
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {resolvidos_schema};")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {abertos_schema};")
        cur.execute(
            f"DELETE FROM {resolvidos_schema}.{resolvidos_table} WHERE id = ANY(%s);",
            (ids,),
        )
        cur.execute(
            f"DELETE FROM {abertos_schema}.{abertos_table} WHERE id = ANY(%s);", (ids,)
        )
        return len(ids)


def main() -> None:
    base_url = env_str("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
    token = env_str("MOVIDESK_TOKEN")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    timeout = env_int("MOVIDESK_TIMEOUT", 30)

    schema = env_str("PG_SCHEMA", "public") or "public"
    merged_table = env_str("PG_TABLE_MERGED", "tickets_merged") or "tickets_merged"

    control_table = env_str("PG_CONTROL_TABLE", "range_scan_control_merged") or "range_scan_control_merged"

    resolvidos_schema = env_str("PG_SCHEMA_RESOLVIDOS", "public") or "public"
    resolvidos_table = env_str("PG_TABLE_RESOLVIDOS", "tickets_resolvidos") or "tickets_resolvidos"

    abertos_schema = env_str("PG_SCHEMA_ABERTOS", "public") or "public"
    abertos_table = env_str("PG_TABLE_ABERTOS", "tickets_abertos") or "tickets_abertos"

    start_id = env_int("START_ID", 0)
    end_id = env_int("END_ID", 0)

    # Se não passar START/END, usa o controle do banco: começa do último id e desce.
    range_size = env_int("RANGE_SIZE", 300)
    sleep_ms = env_int("SLEEP_MS", 0)

    delete_from_other = parse_bool(env_str("DELETE_FROM_OTHER_TABLES"), default=True)

    sess = movidesk_session()
    conn = pg_connect()
    conn.autocommit = False

    try:
        ensure_control_table(conn, schema, control_table)
        last_processed = get_last_processed(conn, schema, control_table)

        if start_id > 0 and end_id > 0:
            next_id = end_id
            min_id = start_id
        else:
            next_id = last_processed if last_processed > 0 else 0
            min_id = 1

        if next_id == 0:
            # fallback: se não tem controle inicial, começa do END_ID se houver; senão não roda.
            if end_id > 0:
                next_id = end_id
            else:
                raise RuntimeError(
                    "Não há controle inicial (range_scan_control_merged id=0) e END_ID não foi informado."
                )

        total_checked = 0
        total_rel = 0
        total_upserted = 0
        total_deleted = 0

        status_counts: Dict[str, int] = {}

        log.info(
            "start base_url=%s schema=%s merged_table=%s control_table=%s next_id=%s min_id=%s range_size=%s",
            base_url,
            schema,
            merged_table,
            control_table,
            next_id,
            min_id,
            range_size,
        )

        while next_id >= min_id:
            batch_ids = list(range(max(min_id, next_id - range_size + 1), next_id + 1))
            batch_ids.reverse()  # do maior pro menor

            checked = 0
            rel = 0
            upserted = 0
            deleted = 0

            for tid in batch_ids:
                checked += 1
                data, status = movidesk_get_merged(sess, base_url, token, tid, timeout)
                s = str(status) if status is not None else "None"
                status_counts[s] = status_counts.get(s, 0) + 1

                if status == 200 and isinstance(data, dict) and data:
                    # Considera que retorno "ok" é quando tem pelo menos ticketId/mergedTicketId/id
                    if any(k in data for k in ("id", "ticketId", "mergedTicketId")):
                        rel += 1
                        upserted += upsert_merged(conn, schema, merged_table, [data])

                        if delete_from_other:
                            del_ids = []
                            for k in ("ticketId", "mergedTicketId"):
                                v = normalize_int(data.get(k))
                                if v is not None:
                                    del_ids.append(v)
                            if del_ids:
                                deleted += delete_from_other_tables(
                                    conn,
                                    resolvidos_schema,
                                    resolvidos_table,
                                    abertos_schema,
                                    abertos_table,
                                    del_ids,
                                )

                if sleep_ms > 0:
                    time.sleep(sleep_ms / 1000.0)

            last_processed_run = batch_ids[-1]  # o menor do lote
            update_last_processed(conn, schema, control_table, int(last_processed_run))
            conn.commit()

            next_id = last_processed_run - 1

            total_checked += checked
            total_rel += rel
            total_upserted += upserted
            total_deleted += deleted

            log.info(
                "progress next_id=%s last_processed=%s checked=%d upsert=%d deleted=%d",
                next_id,
                last_processed_run,
                checked,
                upserted,
                deleted,
            )

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
