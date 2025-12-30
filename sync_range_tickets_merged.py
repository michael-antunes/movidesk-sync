import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


def _env_str(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _env_int(k: str, default: int) -> int:
    v = _env_str(k)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_float(k: str, default: float) -> float:
    v = _env_str(k)
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(k: str, default: bool = False) -> bool:
    v = _env_str(k)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


def _qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{_qident(schema)}.{_qident(table)}"


def pg_connect():
    dsn = _env_str("NEON_DSN") or _env_str("DATABASE_URL")
    if not dsn:
        raise RuntimeError("NEON_DSN não definido")
    return psycopg2.connect(dsn)


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _to_int_list(ids: Any) -> List[int]:
    out: List[int] = []
    if isinstance(ids, list):
        for x in ids:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out

    if ids is None:
        return out

    s = str(ids).strip()
    if not s:
        return out

    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    for p in parts:
        if p.isdigit():
            out.append(int(p))
    return out


def movidesk_get_merged(s: requests.Session, base_url: str, token: str, ticket_id: int, timeout: int = 30) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}
    r = s.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk {r.status_code}: {r.text[:500]}")
    data = r.json()
    return data if isinstance(data, dict) else None


def normalize_to_rows(raw: Dict[str, Any]) -> List[Tuple[int, int, Optional[datetime], Any]]:
    principal = raw.get("ticketId") or raw.get("ticketID") or raw.get("id")
    if principal is None:
        return []
    try:
        principal_id = int(principal)
    except Exception:
        return []

    merged_ids = _to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
    if not merged_ids:
        return []

    dt_val = raw.get("mergedDate") or raw.get("performedAt") or raw.get("date") or raw.get("lastUpdate") or raw.get("last_update")
    merged_at = _parse_dt(dt_val)

    payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))

    rows: List[Tuple[int, int, Optional[datetime], Any]] = []
    for src in merged_ids:
        rows.append((int(src), principal_id, merged_at, payload))
    return rows


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], Any]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_into_id, merged_at, raw_payload)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_into_id = EXCLUDED.merged_into_id,
            merged_at = COALESCE(EXCLUDED.merged_at, {qname(schema, table)}.merged_at),
            raw_payload = EXCLUDED.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def read_control(conn, schema: str, control_table: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id_inicial, id_final, id_atual_merged
            FROM {qname(schema, control_table)}
            LIMIT 1
            """
        )
        r = cur.fetchone()
        if not r:
            return None
        return {"id_inicial": r[0], "id_final": r[1], "id_atual_merged": r[2]}


def update_control(conn, schema: str, control_table: str, *, id_inicial=None, id_final=None, id_atual_merged=None):
    sets = []
    params = []
    if id_inicial is not None:
        sets.append("id_inicial=%s")
        params.append(id_inicial)
    if id_final is not None:
        sets.append("id_final=%s")
        params.append(id_final)
    if id_atual_merged is not None:
        sets.append("id_atual_merged=%s")
        params.append(id_atual_merged)
    if not sets:
        return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {qname(schema, control_table)} SET {', '.join(sets)}", params)


def bootstrap_bounds(conn, schema: str, resolved_table: str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(ticket_id)::bigint, MIN(ticket_id)::bigint FROM {qname(schema, resolved_table)}")
        mx, mn = cur.fetchone()
        if mx is None or mn is None:
            return None, None
        return int(mx), int(mn)


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
        rows = cur.fetchall()
        return [int(x[0]) for x in rows]


def main():
    logging.basicConfig(level=_env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("merged-range")

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

        id_inicial = ctrl["id_inicial"]
        id_final = ctrl["id_final"]
        id_ptr = ctrl["id_atual_merged"]

        if id_inicial is None or id_final is None:
            mx, mn = bootstrap_bounds(conn, schema, resolved_table)
            if mx is None or mn is None:
                conn.rollback()
                return
            id_inicial = mx
            id_final = mn
            if id_ptr is None:
                id_ptr = id_inicial
            if not dry_run:
                update_control(conn, schema, control_table, id_inicial=id_inicial, id_final=id_final, id_atual_merged=id_ptr)
                conn.commit()

        if id_ptr is None:
            id_ptr = id_inicial

        if id_ptr < id_final:
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

    s = session()
    to_upsert: List[Tuple[int, int, Optional[datetime], Any]] = []
    checked = 0

    for principal_id in batch:
        checked += 1
        raw = movidesk_get_merged(s, base_url, token, principal_id)
        if raw:
            to_upsert.extend(normalize_to_rows(raw))
        if throttle > 0:
            time.sleep(throttle)

    last_ticket_id = batch[-1]
    new_ptr = last_ticket_id - 1
    if new_ptr < id_final:
        new_ptr = id_final - 1

    if dry_run:
        log.info("checked=%d upsert=%d id_ptr=%s->%s", checked, 0, id_ptr, new_ptr)
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        upserted = upsert_rows(conn2, schema, table, to_upsert)
        update_control(conn2, schema, control_table, id_atual_merged=new_ptr)
        conn2.commit()
        log.info("checked=%d upsert=%d id_ptr=%s->%s", checked, upserted, id_ptr, new_ptr)
    finally:
        try:
            conn2.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
