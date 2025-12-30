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


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int = 30) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}
    resp = sess.get(url, params=params, timeout=timeout)
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
        return {"id_inicial": int(r[0]), "id_final": int(r[1]), "id_atual_merged": int(r[2]) if r[2] is not None else None}


def update_control(conn, schema: str, control_table: str, id_atual_merged: int):
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {qname(schema, control_table)} SET id_atual_merged=%s", (id_atual_merged,))


def build_batch(start_id: int, end_id: int, limit: int) -> List[int]:
    if start_id <= end_id:
        return []
    stop = max(end_id, start_id - limit + 1)
    return list(range(start_id, stop - 1, -1))


def upsert_children(conn, schema: str, table: str, rows: Sequence[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_into_id, merged_at, situacao_mesclado, raw_payload, imported_at, last_update, synced_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_into_id = EXCLUDED.merged_into_id,
            merged_at = EXCLUDED.merged_at,
            situacao_mesclado = EXCLUDED.situacao_mesclado,
            raw_payload = EXCLUDED.raw_payload,
            last_update = EXCLUDED.last_update,
            synced_at = EXCLUDED.synced_at
    """
    values = []
    for ticket_id, merged_into_id, merged_at, situacao, raw_payload, imported_at, last_update in rows:
        values.append((ticket_id, merged_into_id, merged_at, situacao, psycopg2.extras.Json(raw_payload), imported_at, last_update, _now()))
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

        id_inicial = ctrl["id_inicial"]
        id_final = ctrl["id_final"]
        id_ptr = ctrl["id_atual_merged"] if ctrl["id_atual_merged"] is not None else id_inicial

        batch = build_batch(id_ptr, id_final, limit)
        conn.rollback()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not batch:
        return

    sess = _session()
    imported_at = _now()
    checked = 0
    principals_with_merges = 0
    child_rows: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any], datetime, Optional[datetime]]] = []

    for principal_id in batch:
        checked += 1
        raw = movidesk_get_merged(sess, base_url, token, principal_id)
        if raw:
            merged_ids = _extract_ids(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
            if merged_ids:
                principals_with_merges += 1
                last_update = _parse_dt(raw.get("lastUpdate") or raw.get("last_update"))
                merged_at = last_update
                for mid in merged_ids:
                    child_rows.append((int(mid), int(principal_id), merged_at, "Sim", raw, imported_at, last_update))
        if throttle > 0:
            time.sleep(throttle)

    new_ptr = batch[-1] - 1
    if new_ptr < id_final:
        new_ptr = id_final

    if dry_run:
        log.info("checked=%d principals_with_merges=%d upserted=0 id_ptr=%s->%s", checked, principals_with_merges, batch[0], new_ptr)
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        upserted = upsert_children(conn2, schema, table, child_rows)
        update_control(conn2, schema, control_table, new_ptr)
        conn2.commit()
        log.info("checked=%d principals_with_merges=%d upserted=%d id_ptr=%s->%s", checked, principals_with_merges, upserted, batch[0], new_ptr)
    finally:
        try:
            conn2.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
