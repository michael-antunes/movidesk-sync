import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras
import requests


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _now() -> datetime:
    return datetime.now(timezone.utc)


def qname(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def pg_connect():
    dsn = _env_str("NEON_DSN") or _env_str("DATABASE_URL")
    if not dsn:
        raise RuntimeError("NEON_DSN não definido")
    return psycopg2.connect(dsn)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _extract_ids(value: Any) -> List[int]:
    if value is None:
        return []
    if isinstance(value, list):
        out = []
        for x in value:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    s = str(value)
    return [int(x) for x in re.findall(r"\d+", s)]


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}
    r = sess.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk {r.status_code}: {r.text[:500]}")
    data = r.json()
    return data if isinstance(data, dict) else None


def read_control(conn, schema: str, control_table: str) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id_inicial::bigint, id_final::bigint, id_atual_merged::bigint
            FROM {qname(schema, control_table)}
            LIMIT 1
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
        cur.execute(f"UPDATE {qname(schema, control_table)} SET id_atual_merged=%s", (new_ptr,))


def build_batch(id_ptr: int, id_final: int, limit: int) -> List[int]:
    if id_ptr <= id_final:
        return []
    stop = max(id_final, id_ptr - limit + 1)
    return list(range(id_ptr, stop - 1, -1))


def fetch_existing_mesclados(conn, schema: str, table: str, ids: List[int]) -> Set[int]:
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT ticket_id::bigint FROM {qname(schema, table)} WHERE ticket_id = ANY(%s)",
            (ids,),
        )
        return {int(x[0]) for x in cur.fetchall()}


def insert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_into_id, merged_at, situacao_mesclado, raw_payload, imported_at, last_update)
        VALUES %s
        ON CONFLICT (ticket_id) DO NOTHING
    """
    values = []
    for ticket_id, merged_into_id, merged_at, situacao, raw_payload, imported_at, last_update in rows:
        values.append((ticket_id, merged_into_id, merged_at, situacao, psycopg2.extras.Json(raw_payload), imported_at, last_update))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    return len(values)


def delete_from_other_tables(conn, abertos_schema: str, abertos_table: str, resolvidos_schema: str, resolvidos_table: str, ids: List[int]):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {qname(abertos_schema, abertos_table)} WHERE ticket_id = ANY(%s)", (ids,))
        cur.execute(f"DELETE FROM {qname(resolvidos_schema, resolvidos_table)} WHERE ticket_id = ANY(%s)", (ids,))


def main():
    token = _env_str("MOVIDESK_TOKEN")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    base_url = _env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    http_timeout = _env_int("HTTP_TIMEOUT", 60)
    limit = _env_int("LIMIT", 80)
    rpm = _env_float("RPM", 9.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    dry_run = _env_bool("DRY_RUN", False)

    db_schema = _env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table_mesclados = _env_str("TABLE_NAME", "tickets_mesclados")
    control_table = _env_str("CONTROL_TABLE", "range_scan_control")
    resolvidos_table = _env_str("RESOLVED_TABLE", "tickets_resolvidos_detail")

    abertos_schema = _env_str("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = _env_str("ABERTOS_TABLE", "tickets_abertos")

    conn = pg_connect()
    conn.autocommit = False
    try:
        _, id_final, id_ptr = read_control(conn, db_schema, control_table)
        batch = build_batch(id_ptr, id_final, limit)
        conn.rollback()
    finally:
        conn.close()

    if not batch:
        return

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    child_map: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]] = []
    all_children: List[int] = []
    imported_at = _now()

    for principal_id in batch:
        raw = movidesk_get_merged(sess, base_url, token, principal_id, http_timeout)
        if raw:
            merged_ids = _extract_ids(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
            if merged_ids:
                last_update = _parse_dt(raw.get("lastUpdate") or raw.get("last_update"))
                merged_at = last_update
                for mid in merged_ids:
                    all_children.append(int(mid))
                    child_map.append((int(mid), int(principal_id), merged_at, "Sim", raw, imported_at, last_update))
        if throttle > 0:
            time.sleep(throttle)

    if not all_children:
        new_ptr = batch[-1] - 1
        if new_ptr < id_final:
            new_ptr = id_final
        if not dry_run:
            conn2 = pg_connect()
            conn2.autocommit = False
            try:
                update_ptr(conn2, db_schema, control_table, new_ptr)
                conn2.commit()
            finally:
                conn2.close()
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        existing = fetch_existing_mesclados(conn2, db_schema, table_mesclados, all_children)
        to_insert = [r for r in child_map if r[0] not in existing]

        if not dry_run:
            insert_mesclados(conn2, db_schema, table_mesclados, to_insert)
            delete_from_other_tables(conn2, abertos_schema, abertos_table, db_schema, resolvidos_table, all_children)

            new_ptr = batch[-1] - 1
            if new_ptr < id_final:
                new_ptr = id_final
            update_ptr(conn2, db_schema, control_table, new_ptr)
            conn2.commit()
        else:
            conn2.rollback()
    finally:
        conn2.close()


if __name__ == "__main__":
    main()
