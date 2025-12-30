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


def _parse_dt(v: Any) -> Optional[datetime]:
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


def _to_int_list(v: Any) -> List[int]:
    out: List[int] = []
    if v is None:
        return out
    if isinstance(v, list):
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    s = str(v).strip()
    if not s:
        return out
    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    for p in parts:
        if p.isdigit():
            out.append(int(p))
    return out


def movidesk_get_merged(sess: requests.Session, base_url: str, token: str, principal_id: int, timeout: int) -> Optional[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    params = {"token": token, "id": str(principal_id)}
    r = sess.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk {r.status_code}: {r.text[:500]}")
    data = r.json()
    return data if isinstance(data, dict) else None


def read_control(conn, schema: str, control_table: str) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT id_inicial::bigint, id_final::bigint, id_atual_merged::bigint FROM {qname(schema, control_table)} LIMIT 1")
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


def upsert_mesclados(conn, schema: str, table: str, rows: List[Tuple[int, int, Optional[datetime], str, Any]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_into_id, merged_at, situacao_mesclado, raw_payload)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_into_id = EXCLUDED.merged_into_id,
            merged_at = COALESCE(EXCLUDED.merged_at, {qname(schema, table)}.merged_at),
            situacao_mesclado = EXCLUDED.situacao_mesclado,
            raw_payload = EXCLUDED.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def delete_from_other_tables(conn, resolvidos_schema: str, resolvidos_table: str, abertos_schema: str, abertos_table: str, ids: List[int]):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {qname(resolvidos_schema, resolvidos_table)} WHERE ticket_id = ANY(%s)", (ids,))
        cur.execute(f"DELETE FROM {qname(abertos_schema, abertos_table)} WHERE ticket_id = ANY(%s)", (ids,))


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

    resolvidos_schema = _env_str("RESOLVIDOS_SCHEMA", "visualizacao_resolvidos")
    resolvidos_table = _env_str("RESOLVIDOS_TABLE", "tickets_resolvidos_detail")
    abertos_schema = _env_str("ABERTOS_SCHEMA", "visualizacao_atual")
    abertos_table = _env_str("ABERTOS_TABLE", "tickets_abertos")

    limit = _env_int("LIMIT", 80)
    rpm = _env_float("RPM", 9.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    timeout = _env_int("HTTP_TIMEOUT", 30)
    dry_run = _env_bool("DRY_RUN", False)

    conn = pg_connect()
    conn.autocommit = False
    try:
        id_inicial, id_final, id_ptr = read_control(conn, schema, control_table)
        conn.rollback()
    finally:
        conn.close()

    if id_ptr <= id_final:
        return

    stop = max(id_final, id_ptr - limit + 1)
    batch = list(range(id_ptr, stop - 1, -1))
    new_ptr = batch[-1] - 1
    if new_ptr < id_final:
        new_ptr = id_final

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    rows: List[Tuple[int, int, Optional[datetime], str, Any]] = []
    all_mesclados: List[int] = []
    checked = 0

    for principal_id in batch:
        checked += 1
        raw = movidesk_get_merged(sess, base_url, token, principal_id, timeout)
        if raw:
            merged_ids = _to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
            if merged_ids:
                merged_at = _parse_dt(raw.get("mergedDate") or raw.get("performedAt") or raw.get("date") or raw.get("lastUpdate") or raw.get("last_update"))
                payload = psycopg2.extras.Json(raw, dumps=lambda o: json.dumps(o, ensure_ascii=False))
                for mid in merged_ids:
                    all_mesclados.append(int(mid))
                    rows.append((int(mid), int(principal_id), merged_at, "Sim", payload))
        if throttle > 0:
            time.sleep(throttle)

    if dry_run:
        log.info("checked=%d upsert=0 id_ptr=%s->%s", checked, id_ptr, new_ptr)
        return

    conn2 = pg_connect()
    conn2.autocommit = False
    try:
        upserted = upsert_mesclados(conn2, schema, table, rows)
        delete_from_other_tables(conn2, resolvidos_schema, resolvidos_table, abertos_schema, abertos_table, list(set(all_mesclados)))
        update_ptr(conn2, schema, control_table, new_ptr)
        conn2.commit()
        log.info("checked=%d upsert=%d id_ptr=%s->%s", checked, upserted, id_ptr, new_ptr)
    except Exception:
        conn2.rollback()
        raise
    finally:
        conn2.close()


if __name__ == "__main__":
    main()
