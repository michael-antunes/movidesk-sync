#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests


LOG = logging.getLogger("range_scan")


def _env_str(*names: str, default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v.strip()
    return default


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


def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def qname(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


@dataclass
class Settings:
    token: str
    base_url: str
    db_schema: str
    table_name: str
    control_table: str
    resolved_table: str
    resolved_id_col: str
    resolved_date_col: str
    limit: int
    rpm: float
    dry_run: bool

    @property
    def throttle_seconds(self) -> float:
        return 60.0 / self.rpm if self.rpm > 0 else 0.0


def load_settings() -> Settings:
    token = _env_str("MOVIDESK_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN")

    return Settings(
        token=token,
        base_url=_env_str("MOVIDESK_BASE_URL", default="https://api.movidesk.com/public/v1"),
        db_schema=_env_str("DB_SCHEMA", default="visualizacao_resolvidos"),
        table_name=_env_str("TABLE_NAME", default="tickets_mesclados"),
        control_table=_env_str("CONTROL_TABLE", default="range_scan_control"),
        resolved_table=_env_str("RESOLVED_TABLE", default="tickets_resolvidos_detail"),
        resolved_id_col=_env_str("RESOLVED_ID_COL", default="ticket_id"),
        resolved_date_col=_env_str("RESOLVED_DATE_COL", default="updated_at"),
        limit=_env_int("LIMIT", 80),
        rpm=_env_float("RPM", 9.0),
        dry_run=_env_bool("DRY_RUN", False),
    )


def pg_connect():
    dsn = _env_str("NEON_DSN", "DATABASE_URL")
    if not dsn:
        raise RuntimeError("Falta NEON_DSN (ou DATABASE_URL)")
    return psycopg2.connect(dsn)


@dataclass
class ControlRow:
    ctid: str
    data_inicio: datetime
    data_fim: datetime
    ultima_data_validada: Optional[datetime]
    ultima_data_validada_merged: Optional[datetime]
    id_inicial: Optional[int]
    id_final: Optional[int]
    id_atual: Optional[int]
    id_atual_merged: Optional[int]


def read_latest_control(conn, schema: str, control_table: str) -> Optional[ControlRow]:
    sql = f"""
        SELECT ctid::text,
               data_inicio, data_fim,
               ultima_data_validada, ultima_data_validada_merged,
               id_inicial, id_final,
               id_atual, id_atual_merged
          FROM {qname(schema, control_table)}
         ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
         LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return None
        return ControlRow(
            ctid=row[0],
            data_inicio=_parse_dt(row[1]) or _dt_now(),
            data_fim=_parse_dt(row[2]) or _dt_now(),
            ultima_data_validada=_parse_dt(row[3]),
            ultima_data_validada_merged=_parse_dt(row[4]),
            id_inicial=row[5],
            id_final=row[6],
            id_atual=row[7],
            id_atual_merged=row[8],
        )


def update_control(
    conn,
    schema: str,
    control_table: str,
    ctid: str,
    *,
    data_inicio: Optional[datetime] = None,
    data_fim: Optional[datetime] = None,
    ultima_data_validada: Optional[datetime] = None,
    ultima_data_validada_merged: Optional[datetime] = None,
    id_inicial: Optional[int] = None,
    id_final: Optional[int] = None,
    id_atual: Optional[int] = None,
    id_atual_merged: Optional[int] = None,
) -> str:
    sets: List[str] = []
    params: List[Any] = []

    def add(col: str, val: Any) -> None:
        sets.append(f"{col} = %s")
        params.append(val)

    add("updated_at", _dt_now())

    if data_inicio is not None:
        add("data_inicio", data_inicio)
    if data_fim is not None:
        add("data_fim", data_fim)
    if ultima_data_validada is not None:
        add("ultima_data_validada", ultima_data_validada)
    if ultima_data_validada_merged is not None:
        add("ultima_data_validada_merged", ultima_data_validada_merged)
    if id_inicial is not None:
        add("id_inicial", id_inicial)
    if id_final is not None:
        add("id_final", id_final)
    if id_atual is not None:
        add("id_atual", id_atual)
    if id_atual_merged is not None:
        add("id_atual_merged", id_atual_merged)

    if not sets:
        return ctid

    sql = f"""
        UPDATE {qname(schema, control_table)}
           SET {", ".join(sets)}
         WHERE ctid::text = %s
     RETURNING ctid::text
    """
    params.append(ctid)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        r = cur.fetchone()
        if not r:
            return ctid
        return r[0]


def bootstrap_ids_from_resolved(
    conn,
    schema: str,
    resolved_table: str,
    id_col: str,
) -> Tuple[Optional[int], Optional[int]]:
    sql = f"""
        SELECT MAX({id_col})::bigint AS max_id,
               MIN({id_col})::bigint AS min_id
        FROM {qname(schema, resolved_table)}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        r = cur.fetchone()
        if not r:
            return None, None
        return r[0], r[1]



def fetch_next_batch_ids(
    conn,
    schema: str,
    resolved_table: str,
    id_col: str,
    *,
    from_id: int,
    to_id: int,
    limit: int,
) -> List[int]:
    sql = f"""
        SELECT {id_col}::bigint AS ticket_id
        FROM {qname(schema, resolved_table)}
        WHERE {id_col}::bigint <= %s
          AND {id_col}::bigint >= %s
        ORDER BY {id_col}::bigint DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (from_id, to_id, limit))
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]



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


def normalize_merged_record(raw: Dict[str, Any]) -> Optional[Tuple[int, int, str, List[int], Optional[datetime]]]:
    tid = raw.get("ticketId") or raw.get("ticketID") or raw.get("id")
    if tid is None:
        return None
    ticket_id = int(tid)

    merged_tickets = raw.get("mergedTickets")
    if merged_tickets is None:
        return None
    merged_tickets_i = int(merged_tickets)
    if merged_tickets_i <= 0:
        return None

    ids = raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList")
    if ids is None:
        return None

    merged_ids: List[int] = []
    if isinstance(ids, list):
        for x in ids:
            try:
                merged_ids.append(int(x))
            except Exception:
                pass
    else:
        s = str(ids).strip()
        if not s:
            return None
        parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
        merged_ids = [int(p) for p in parts if p.isdigit()]

    if not merged_ids:
        return None

    merged_ids_text = json.dumps(merged_ids, ensure_ascii=False)
    last_update = _parse_dt(raw.get("lastUpdate") or raw.get("last_update"))
    return (ticket_id, merged_tickets_i, merged_ids_text, merged_ids, last_update)


def upsert_merged_rows(
    conn,
    schema: str,
    table: str,
    rows: Sequence[Tuple[int, int, str, List[int], Optional[datetime]]],
    *,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        return 0

    sql = f"""
        INSERT INTO {qname(schema, table)}
            (ticket_id, merged_tickets, merged_tickets_ids, merged_ticket_ids_arr, last_update, synced_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            merged_ticket_ids_arr = EXCLUDED.merged_ticket_ids_arr,
            last_update = EXCLUDED.last_update,
            synced_at = EXCLUDED.synced_at
    """

    values = []
    for ticket_id, merged_tickets, merged_ids_text, merged_ids_arr, last_update in rows:
        values.append(
            (
                int(ticket_id),
                int(merged_tickets),
                merged_ids_text,
                merged_ids_arr,
                last_update,
                _dt_now(),
            )
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    return len(values)


def _safe_rollback(conn) -> None:
    try:
        if conn and getattr(conn, "closed", 1) == 0:
            conn.rollback()
    except Exception:
        pass


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = load_settings()

    conn = pg_connect()
    conn.autocommit = False
    try:
        control = read_latest_control(conn, cfg.db_schema, cfg.control_table)
        if not control:
            _safe_rollback(conn)
            return

        id_inicial = control.id_inicial
        id_final = control.id_final
        id_ptr = control.id_atual_merged
        if id_ptr is None and id_inicial is not None:
            id_ptr = id_inicial

        if id_inicial is None or id_final is None or id_ptr is None:
            max_id, min_id = bootstrap_ids_from_resolved(
                conn,
                cfg.db_schema,
                cfg.resolved_table,
                cfg.resolved_id_col,
            )
            if max_id is None or min_id is None:
                _safe_rollback(conn)
                return

            id_inicial = int(max_id) if id_inicial is None else int(id_inicial)
            id_final = int(min_id) if id_final is None else int(id_final)

            if id_ptr is None:
                id_ptr = id_inicial

            if not cfg.dry_run:
                control.ctid = update_control(
                    conn,
                    cfg.db_schema,
                    cfg.control_table,
                    control.ctid,
                    id_inicial=id_inicial,
                    id_final=id_final,
                    id_atual_merged=id_ptr,
                )
                conn.commit()

        if id_ptr < id_final:
            _safe_rollback(conn)
            return

        batch = fetch_next_batch_ids(
            conn,
            cfg.db_schema,
            cfg.resolved_table,
            cfg.resolved_id_col,
            from_id=id_ptr,
            to_id=id_final,
            limit=cfg.limit,
        )
        if not batch:
            _safe_rollback(conn)
            return

        _safe_rollback(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    LOG.info(
        "scanner iniciando | schema=%s tabela=%s control=%s | id_inicial=%s id_final=%s id_ptr=%s | limit=%s | rpm=%.2f | throttle=%.2fs | dry_run=%s",
        cfg.db_schema,
        cfg.table_name,
        cfg.control_table,
        id_inicial,
        id_final,
        id_ptr,
        cfg.limit,
        cfg.rpm,
        cfg.throttle_seconds,
        cfg.dry_run,
    )

    session = _session()
    to_upsert: List[Tuple[int, int, str, List[int], Optional[datetime]]] = []
    checked = 0
    merged_found = 0

    for ticket_id in batch:
        checked += 1
        raw = movidesk_get_merged(session, cfg.base_url, cfg.token, ticket_id)
        if raw:
            norm = normalize_merged_record(raw)
            if norm:
                to_upsert.append(norm)
                merged_found += 1

        if cfg.throttle_seconds > 0:
            time.sleep(cfg.throttle_seconds)

    last_ticket_id = batch[-1]
    new_ptr = last_ticket_id - 1

    if cfg.dry_run:
        LOG.info(
            "scanner concluído | checked=%d merged_found=%d upserted=%d | id_ptr %s -> %s",
            checked,
            merged_found,
            0,
            id_ptr,
            new_ptr,
        )
        return

    for attempt in (1, 2):
        conn2 = None
        try:
            conn2 = pg_connect()
            conn2.autocommit = False

            upserted = upsert_merged_rows(conn2, cfg.db_schema, cfg.table_name, to_upsert, dry_run=cfg.dry_run)

            control2 = read_latest_control(conn2, cfg.db_schema, cfg.control_table)
            if not control2:
                _safe_rollback(conn2)
                return

            update_control(
                conn2,
                cfg.db_schema,
                cfg.control_table,
                control2.ctid,
                id_atual_merged=new_ptr,
                id_inicial=control2.id_inicial,
                id_final=control2.id_final,
            )

            conn2.commit()

            LOG.info(
                "scanner concluído | checked=%d merged_found=%d upserted=%d | id_ptr %s -> %s",
                checked,
                merged_found,
                upserted,
                id_ptr,
                new_ptr,
            )
            return

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if conn2 is not None:
                _safe_rollback(conn2)
                try:
                    conn2.close()
                except Exception:
                    pass
            if attempt == 1:
                time.sleep(1.5)
                continue
            raise e
        finally:
            if conn2 is not None:
                try:
                    conn2.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
