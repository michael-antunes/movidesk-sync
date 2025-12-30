import os
import re
import time
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


def _dt_now() -> datetime:
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


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


def movidesk_get_merged(session: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int = 30) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}
    resp = session.get(url, params=params, timeout=timeout)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(f"Movidesk {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    return data if isinstance(data, dict) else None


def read_control(conn, schema: str, control_table: str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT id_inicial, id_final, id_atual_merged FROM {qname(schema, control_table)} LIMIT 1")
        r = cur.fetchone()
        if not r:
            return None
        return {"id_inicial": r[0], "id_final": r[1], "id_atual_merged": r[2]}


def update_control(conn, schema: str, control_table: str, id_atual_merged: int):
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {qname(schema, control_table)} SET id_atual_merged=%s", (id_atual_merged,))


def fetch_batch_ids(conn, schema: str, resolved_table: str, from_id: int, to_id: int, limit: int) -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT ticket_id::bigint
            FROM {qname(schema, resolved_table)}
            WHERE ticket_id::bigint <= %s
              AND ticket_id::bigint >= %s
            ORDER BY ticket_id::bigint DESC
            LIMIT %s
            """,
            (from_id, to_id, limit),
        )
        return [int(x[0]) for x in cur.fetchall()]


def delete_wrong_principals(conn, schema: str, table: str, principals: Sequence[int]):
    if not principals:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {qname(schema, table)}
            WHERE ticket_id = ANY(%s)
              AND merged_into_id IS NULL
            """,
            (list(principals),),
        )


def upsert_merged(conn, schema: str, table: str, rows: Sequence[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_into_id, merged_at, situacao_mesclado, raw_payload, imported_at, last_update)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_into_id = EXCLUDED.merged_into_id,
            merged_at = EXCLUDED.merged_at,
            situacao_mesclado = EXCLUDED.situacao_mesclado,
            raw_payload = EXCLUDED.raw_payload,
            last_update = EXCLUDED.last_update,
            imported_at = COALESCE({qname(schema, table)}.imported_at, EXCLUDED.imported_at)
    """
    values = []
    for ticket_id, merged_into_id, merged_at, situacao, raw_payload, imported_at, last_update in rows:
        values.append((ticket_id, merged_into_id, merged_at, situacao, psycopg2.extras.Json(raw_payload), imported_at, last_update))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    return len(values)


def main():
    logging.basicConfig(level=_env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("range-merged")

    token = _env_str("MOVIDESK_TOKEN")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido")

    base_url = _env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    schema = _env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table = _env_str("TABLE_NAME", "tickets_mesclados")
    control_table = _env_str("CONTROL_TABLE", "range_scan_control")
    resolved_table = _env_str("RESOLVED_TABLE", "tickets_resolvidos_detail")

    limit = _env_int("LIMIT", 80)
    rpm = _env_float("RPM", 9.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    dry_run = _env_bool("DRY_RUN", False)

    conn = pg_connect()
    conn.autocommit = False
    try:
        ctrl = read_control(conn, schema, control_table)
        if not ctrl:
            conn.rollback()
            return

        id_inicial = int(ctrl["id_inicial"])
        id_final = int(ctrl["id_final"])
        id_ptr = ctrl["id_atual_merged"]
        id_ptr = int(id_ptr) if id_ptr is not None else id_inicial

        if id_ptr <= id_final:
            conn.rollback()
            return

        batch = fetch_batch_ids(conn, schema, resolved_table, id_ptr, id_final, limit)
        conn.rollback()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not batch:
        return

    sess = _session()
    principals_with_merges: List[int] = []
    merged_rows: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]] = []
    checked = 0

    for principal_id in batch:
        checked += 1
        raw = movidesk_get_merged(sess, base_url, token, principal_id)
        if raw:
            merged_ids = _extract_ids(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
            if merged_ids:
                principals_with_merges.append(principal_id)
                last_update = _parse_dt(raw.get("lastUpdate") or raw.get("last_update"))
                merged_at = last_update
                imported_at = _dt_now()
                for mid in merged_ids:
                    payload = {"ticket_id": mid, "merged_into_id": principal_id, "movidesk": raw}
                    merged_rows.append((mid, principal_id, merged_at, "Sim", payload, imported_at, last_update))
        if throttle > 0:
            time.sleep(throttle)

    last_ticket_id = batch[-1]
    new_ptr = last_ticket_id - 1
    if new_ptr < id_final:
        new_ptr = id_final

    if dry_run:
        log.info("checked=%d principals_with_merges=%d inserts=%d id_ptr=%s->%s", checked, len(set(principals_with_merges)), len(merged_rows), id_ptr, new_ptr)
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        delete_wrong_principals(conn2, schema, table, list(set(principals_with_merges)))
        upserted = upsert_merged(conn2, schema, table, merged_rows)
        update_control(conn2, schema, control_table, new_ptr)
        conn2.commit()
        log.info("checked=%d principals_with_merges=%d upserted=%d id_ptr=%s->%s", checked, len(set(principals_with_merges)), upserted, id_ptr, new_ptr)
    finally:
        try:
            conn2.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
