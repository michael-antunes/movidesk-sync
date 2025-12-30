import os
import re
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values, Json


API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

LIMIT = int(os.getenv("LIMIT", "80"))
RPM = float(os.getenv("RPM", "9"))
THROTTLE = 60.0 / RPM if RPM > 0 else 0.0
DRY_RUN = (os.getenv("DRY_RUN", "false") or "").strip().lower() == "true"

SCHEMA = os.getenv("DB_SCHEMA", "visualizacao_resolvidos")
TABLE_MESCLADOS = os.getenv("TABLE_NAME", "tickets_mesclados")
CONTROL_TABLE = os.getenv("CONTROL_TABLE", "range_scan_control")
RESOLVIDOS_TABLE = os.getenv("RESOLVED_TABLE", "tickets_resolvidos_detail")

ABERTOS_SCHEMA = os.getenv("ABERTOS_SCHEMA", "visualizacao_atual")
ABERTOS_TABLE = os.getenv("ABERTOS_TABLE", "tickets_abertos")

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/range-merged"})


def qname(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def md_get_merged(principal_id: int) -> Optional[Dict[str, Any]]:
    url = f"{API_BASE}/tickets/merged"
    params = {"token": TOKEN, "id": str(principal_id)}
    r = S.get(url, params=params, timeout=60)
    if r.status_code == 404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r = S.get(url, params=params, timeout=60)
        if r.status_code == 404:
            return None
    if r.status_code != 200:
        r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else None


def extract_ids(v: Any) -> List[int]:
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
    s = str(v)
    return [int(x) for x in re.findall(r"\d+", s)]


def parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def read_control(conn) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT id_inicial::bigint, id_final::bigint, id_atual_merged::bigint FROM {qname(SCHEMA, CONTROL_TABLE)} LIMIT 1")
        r = cur.fetchone()
        if not r:
            raise RuntimeError("range_scan_control vazio")
        id_inicial = int(r[0])
        id_final = int(r[1])
        id_ptr = int(r[2]) if r[2] is not None else id_inicial
        return id_inicial, id_final, id_ptr


def update_ptr(conn, new_ptr: int):
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {qname(SCHEMA, CONTROL_TABLE)} SET id_atual_merged=%s", (new_ptr,))


def fetch_existing_mesclados(conn, ids: List[int]) -> set:
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(f"SELECT ticket_id::bigint FROM {qname(SCHEMA, TABLE_MESCLADOS)} WHERE ticket_id = ANY(%s)", (ids,))
        return {int(x[0]) for x in cur.fetchall()}


def upsert_mesclados(conn, rows: List[Tuple[int, int, Optional[datetime], Dict[str, Any], datetime, Optional[datetime]]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {qname(SCHEMA, TABLE_MESCLADOS)}
            (ticket_id, merged_into_id, merged_at, situacao_mesclado, raw_payload, imported_at, last_update)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_into_id = EXCLUDED.merged_into_id,
            merged_at      = COALESCE(EXCLUDED.merged_at, {qname(SCHEMA, TABLE_MESCLADOS)}.merged_at),
            raw_payload    = EXCLUDED.raw_payload,
            imported_at    = NOW(),
            last_update    = EXCLUDED.last_update
    """
    values = []
    for ticket_id, merged_into_id, merged_at, raw_payload, imported_at, last_update in rows:
        values.append((ticket_id, merged_into_id, merged_at, "Sim", Json(raw_payload), imported_at, last_update))
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=300)
    return len(values)


def delete_from_abertos_resolvidos(conn, ids: List[int]):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {qname(ABERTOS_SCHEMA, ABERTOS_TABLE)} WHERE ticket_id = ANY(%s)", (ids,))
        cur.execute(f"DELETE FROM {qname(SCHEMA, RESOLVIDOS_TABLE)} WHERE ticket_id = ANY(%s)", (ids,))


def main():
    id_inicial = None
    id_final = None
    id_ptr = None

    with psycopg2.connect(DSN) as conn:
        id_inicial, id_final, id_ptr = read_control(conn)

    if id_ptr <= id_final:
        return

    stop = max(id_final, id_ptr - LIMIT + 1)
    batch = list(range(id_ptr, stop - 1, -1))
    new_ptr = batch[-1] - 1
    if new_ptr < id_final:
        new_ptr = id_final

    imported_at = now_utc()
    rows_to_upsert = []
    all_mesclados = []

    for principal_id in batch:
        raw = md_get_merged(principal_id)
        if raw:
            merged_ids = extract_ids(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsId"))
            if merged_ids:
                last_update = parse_dt(raw.get("lastUpdate") or raw.get("last_update"))
                merged_at = last_update
                for mid in merged_ids:
                    mid = int(mid)
                    all_mesclados.append(mid)
                    payload = raw
                    rows_to_upsert.append((mid, int(principal_id), merged_at, payload, imported_at, last_update))
        if THROTTLE > 0:
            time.sleep(THROTTLE)

    if DRY_RUN:
        return

    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        try:
            existing = fetch_existing_mesclados(conn, all_mesclados)
            to_write = [r for r in rows_to_upsert if r[0] not in existing]
            upsert_mesclados(conn, to_write)
            delete_from_abertos_resolvidos(conn, all_mesclados)
            update_ptr(conn, new_ptr)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


if __name__ == "__main__":
    main()
