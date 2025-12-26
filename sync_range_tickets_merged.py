#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sync_range_tickets_merged.py

Scanner incremental para completar/backfill de tickets mesclados.

Como funciona:
- Lê a linha mais recente em {DB_SCHEMA}.{CONTROL_TABLE} (default: visualizacao_resolvidos.range_scan_control).
- Usa data_inicio/data_fim para restringir os tickets “base” (default: visualizacao_resolvidos.tickets_resolvidos_detail).
- Varre em ordem decrescente do cursor id_atual_merged até id_final, processando LIMIT tickets por execução.
- Para cada ticket_id, consulta Movidesk /tickets/merged?id=<ticket_id> e, se tiver mesclagem, faz UPSERT em tickets_mesclados.
- Atualiza id_atual_merged no controle ao final (cursor = menor_id_processado - 1) e ultima_data_validada = now().

Env vars:
- MOVIDESK_TOKEN ou MOVI_TOKEN: token Movidesk
- NEON_DSN ou DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD(/PGPORT): conexão Postgres
- DB_SCHEMA: schema (default: visualizacao_resolvidos)
- TABLE_NAME: tabela destino (default: tickets_mesclados)
- CONTROL_TABLE: tabela de controle (default: range_scan_control)
- RESOLVED_TABLE: tabela base (default: tickets_resolvidos_detail)
- RESOLVED_ID_COL / RESOLVED_DATE_COL: opcional (auto-detecta quando possível)
- LIMIT: default 80
- RPM: default 9 (throttle 60/RPM)
- DRY_RUN: true/false (default false)
- BASE_URL: default https://api.movidesk.com/public/v1
"""

from __future__ import annotations

import os
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql


LOG = logging.getLogger("range_scan")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return float(v)


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return int(float(v))


@dataclass
class Settings:
    token: str
    base_url: str

    db_schema: str
    table_name: str
    control_table: str
    resolved_table: str

    resolved_id_col: Optional[str]
    resolved_date_col: Optional[str]

    limit: int
    rpm: float
    dry_run: bool


def load_settings() -> Settings:
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVI_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVI_TOKEN).")

    return Settings(
        token=token,
        base_url=os.getenv("BASE_URL", "https://api.movidesk.com/public/v1").rstrip("/"),

        db_schema=os.getenv("DB_SCHEMA", "visualizacao_resolvidos"),
        table_name=os.getenv("TABLE_NAME", "tickets_mesclados"),
        control_table=os.getenv("CONTROL_TABLE", "range_scan_control"),
        resolved_table=os.getenv("RESOLVED_TABLE", "tickets_resolvidos_detail"),

        resolved_id_col=os.getenv("RESOLVED_ID_COL") or None,
        resolved_date_col=os.getenv("RESOLVED_DATE_COL") or None,

        limit=env_int("LIMIT", 80),
        rpm=env_float("RPM", 9.0),
        dry_run=env_bool("DRY_RUN", False),
    )


def pg_connect() -> psycopg2.extensions.connection:
    """
    Compatível com o workflow do repo:
    - usa NEON_DSN (secret) se existir
    - senão DATABASE_URL
    - senão PGHOST/PGDATABASE/PGUSER/PGPASSWORD(/PGPORT)
    """
    dsn = os.getenv("NEON_DSN") or os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not all([host, db, user, password]):
        raise RuntimeError("Faltam variáveis de Postgres. Use NEON_DSN ou DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    sslmode = os.getenv("PGSSLMODE")
    conn_kwargs: Dict[str, Any] = dict(host=host, dbname=db, user=user, password=password, port=port)
    if sslmode:
        conn_kwargs["sslmode"] = sslmode

    return psycopg2.connect(**conn_kwargs)


def ensure_schema(conn: psycopg2.extensions.connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.commit()


def ensure_target_table(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    ddl = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {}.{} (
            ticket_id BIGINT PRIMARY KEY,
            merged_tickets INTEGER,
            merged_tickets_ids TEXT,
            merged_ticket_ids_arr BIGINT[],
            last_update TIMESTAMPTZ,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).format(sql.Identifier(schema), sql.Identifier(table))

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


class MovideskClient:
    def __init__(self, token: str, base_url: str, timeout: int = 30) -> None:
        self.token = token
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

    def get_ticket_merged_by_id(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/tickets/merged"
        params = {"token": self.token, "id": str(ticket_id)}
        r = self.session.get(url, params=params, timeout=self.timeout)

        if r.status_code == 404:
            return None
        r.raise_for_status()

        data = r.json()
        if not data or (isinstance(data, dict) and not data.get("ticketId") and not data.get("mergedTicketsIds") and not data.get("mergedTickets")):
            return None
        if isinstance(data, list):
            return data[0] if data else None
        return data


@dataclass
class ControlRow:
    data_inicio: datetime
    data_fim: datetime
    ultima_data_validada: Optional[datetime]
    id_inicial: int
    id_final: int
    id_atual: Optional[int]
    id_atual_merged: Optional[int]


def parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        s = str(v).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def read_control_latest(conn: psycopg2.extensions.connection, schema: str, control_table: str) -> Optional[ControlRow]:
    # IMPORTANTE: sem coluna "id" (porque sua tabela não tem)
    q = sql.SQL(
        """
        SELECT
            data_inicio,
            data_fim,
            ultima_data_validada,
            id_inicial,
            id_final,
            id_atual,
            id_atual_merged
        FROM {}.{}
        ORDER BY data_inicio DESC
        LIMIT 1
        """
    ).format(sql.Identifier(schema), sql.Identifier(control_table))

    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()

    if not row:
        return None

    return ControlRow(
        data_inicio=row[0],
        data_fim=row[1],
        ultima_data_validada=row[2],
        id_inicial=int(row[3]),
        id_final=int(row[4]),
        id_atual=int(row[5]) if row[5] is not None else None,
        id_atual_merged=int(row[6]) if row[6] is not None else None,
    )


def update_control_cursor(
    conn: psycopg2.extensions.connection,
    schema: str,
    control_table: str,
    key: ControlRow,
    new_id_atual_merged: int,
    ultima_data_validada: Optional[datetime],
) -> None:
    q = sql.SQL(
        """
        UPDATE {}.{}
           SET id_atual_merged = %s,
               ultima_data_validada = %s
         WHERE data_inicio = %s
           AND data_fim = %s
           AND id_inicial = %s
           AND id_final = %s
        """
    ).format(sql.Identifier(schema), sql.Identifier(control_table))

    with conn.cursor() as cur:
        cur.execute(
            q,
            (
                int(new_id_atual_merged),
                ultima_data_validada,
                key.data_inicio,
                key.data_fim,
                int(key.id_inicial),
                int(key.id_final),
            ),
        )
    conn.commit()


def list_table_columns(conn: psycopg2.extensions.connection, schema: str, table: str) -> List[Tuple[str, str]]:
    q = """
        SELECT column_name, data_type
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
         ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [(r[0], r[1]) for r in cur.fetchall()]


def choose_resolved_columns(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    forced_id_col: Optional[str],
    forced_date_col: Optional[str],
) -> Tuple[str, str]:
    cols = list_table_columns(conn, schema, table)
    if not cols:
        raise RuntimeError(f"Tabela base não encontrada ou sem colunas: {schema}.{table}")

    colnames = [c for (c, _t) in cols]

    # ID
    if forced_id_col:
        if forced_id_col not in colnames:
            raise RuntimeError(f"RESOLVED_ID_COL='{forced_id_col}' não existe em {schema}.{table}. Colunas: {colnames}")
        id_col = forced_id_col
    else:
        candidates = []
        for c in colnames:
            cl = c.lower()
            score = 0
            if cl == "ticket_id":
                score += 100
            if cl.endswith("ticket_id"):
                score += 80
            if cl in ("id", "ticketid", "ticket_id"):
                score += 60
            if "ticket" in cl and "id" in cl:
                score += 50
            if cl.endswith("_id"):
                score += 10
            candidates.append((score, c))
        candidates.sort(reverse=True)
        if candidates[0][0] < 40:
            raise RuntimeError(f"Não consegui inferir a coluna de ID em {schema}.{table}. Defina RESOLVED_ID_COL. Colunas: {colnames}")
        id_col = candidates[0][1]

    # DATA
    if forced_date_col:
        if forced_date_col not in colnames:
            raise RuntimeError(f"RESOLVED_DATE_COL='{forced_date_col}' não existe em {schema}.{table}. Colunas: {colnames}")
        date_col = forced_date_col
    else:
        candidates = []
        for c, t in cols:
            cl = c.lower()
            score = 0
            if cl == "data_fim":
                score += 120
            if "resol" in cl or "resolved" in cl:
                score += 100
            if "fech" in cl or "closed" in cl:
                score += 80
            if cl.endswith("_at") or cl.endswith("_date") or cl.endswith("_dt"):
                score += 30
            if "data" in cl:
                score += 20
            if t.lower() in ("timestamp with time zone", "timestamp without time zone", "date"):
                score += 10
            candidates.append((score, c))
        candidates.sort(reverse=True)
        if candidates[0][0] < 40:
            raise RuntimeError(f"Não consegui inferir a coluna de data em {schema}.{table}. Defina RESOLVED_DATE_COL. Colunas: {colnames}")
        date_col = candidates[0][1]

    return id_col, date_col


def fetch_next_ticket_ids(
    conn: psycopg2.extensions.connection,
    schema: str,
    resolved_table: str,
    id_col: str,
    date_col: str,
    data_inicio: datetime,
    data_fim: datetime,
    cursor_id: int,
    id_final: int,
    limit: int,
) -> List[int]:
    q = sql.SQL(
        """
        SELECT {id_col}
          FROM {schema}.{table}
         WHERE {date_col} >= %s
           AND {date_col} <= %s
           AND {id_col} <= %s
           AND {id_col} >= %s
         ORDER BY {id_col} DESC
         LIMIT %s
        """
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(resolved_table),
        id_col=sql.Identifier(id_col),
        date_col=sql.Identifier(date_col),
    )

    with conn.cursor() as cur:
        cur.execute(q, (data_inicio, data_fim, int(cursor_id), int(id_final), int(limit)))
        rows = cur.fetchall()

    return [int(r[0]) for r in rows]


def parse_merged_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in str(s).split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
        else:
            digits = "".join(ch for ch in p if ch.isdigit())
            if digits:
                out.append(int(digits))
    return out or None


def upsert_rows(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    rows: List[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]],
) -> int:
    if not rows:
        return 0

    q = sql.SQL(
        """
        INSERT INTO {}.{} (ticket_id, merged_tickets, merged_tickets_ids, merged_ticket_ids_arr, last_update, synced_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            merged_ticket_ids_arr = EXCLUDED.merged_ticket_ids_arr,
            last_update = EXCLUDED.last_update,
            synced_at = EXCLUDED.synced_at
        """
    ).format(sql.Identifier(schema), sql.Identifier(table))

    nowv = datetime.now(timezone.utc)
    values = [(a, b, c, d, e, nowv) for (a, b, c, d, e) in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, q.as_string(conn), values, page_size=1000)

    conn.commit()
    return len(rows)


def main() -> None:
    setup_logging()
    cfg = load_settings()

    throttle_s = 0.0 if cfg.rpm <= 0 else (60.0 / cfg.rpm)

    LOG.info(
        "scanner iniciando | schema=%s tabela=%s control=%s | limit=%s | rpm=%s | throttle=%.2fs | dry_run=%s",
        cfg.db_schema, cfg.table_name, cfg.control_table, cfg.limit, cfg.rpm, throttle_s, cfg.dry_run
    )

    conn = pg_connect()
    ensure_schema(conn, cfg.db_schema)
    ensure_target_table(conn, cfg.db_schema, cfg.table_name)

    control = read_control_latest(conn, cfg.db_schema, cfg.control_table)
    if not control:
        LOG.info("nenhuma linha em %s.%s -> encerrando OK", cfg.db_schema, cfg.control_table)
        return

    # valida range de datas
    if control.data_fim < control.data_inicio:
        raise RuntimeError(
            f"range_scan_control inválido: data_fim ({control.data_fim}) < data_inicio ({control.data_inicio})."
        )

    cursor = control.id_atual_merged if control.id_atual_merged is not None else control.id_inicial
    cursor = int(cursor)

    resolved_id_col, resolved_date_col = choose_resolved_columns(
        conn, cfg.db_schema, cfg.resolved_table, cfg.resolved_id_col, cfg.resolved_date_col
    )

    LOG.info(
        "controle | periodo=%s..%s | id_inicial=%s id_final=%s | cursor(id_atual_merged)=%s | resolved=%s(%s,%s)",
        control.data_inicio.date(), control.data_fim.date(), control.id_inicial, control.id_final, cursor,
        cfg.resolved_table, resolved_id_col, resolved_date_col
    )

    if cursor < control.id_final:
        LOG.info("scan já concluído (cursor < id_final). Nada a fazer.")
        return

    ticket_ids = fetch_next_ticket_ids(
        conn=conn,
        schema=cfg.db_schema,
        resolved_table=cfg.resolved_table,
        id_col=resolved_id_col,
        date_col=resolved_date_col,
        data_inicio=control.data_inicio,
        data_fim=control.data_fim,
        cursor_id=cursor,
        id_final=control.id_final,
        limit=cfg.limit,
    )

    if not ticket_ids:
        LOG.info("nenhum ticket encontrado para processar nesse range/cursor. Nada a fazer.")
        return

    client = MovideskClient(cfg.token, cfg.base_url)

    rows_to_upsert: List[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]] = []
    processed = 0
    merged_found = 0

    for i, ticket_id in enumerate(ticket_ids, start=1):
        processed += 1
        if i > 1 and throttle_s > 0:
            time.sleep(throttle_s)

        try:
            data = client.get_ticket_merged_by_id(ticket_id)
        except requests.HTTPError as e:
            LOG.warning("API erro ticket_id=%s | %s", ticket_id, str(e))
            continue
        except Exception as e:
            LOG.warning("API exceção ticket_id=%s | %s", ticket_id, str(e))
            continue

        if not data:
            continue

        merged_tickets = data.get("mergedTickets")
        merged_ids_raw = data.get("mergedTicketsIds")
        last_update = parse_ts(data.get("lastUpdate"))

        merged_ids_arr = parse_merged_ids(merged_ids_raw)

        is_merged = False
        try:
            if merged_tickets is not None and int(merged_tickets) > 0:
                is_merged = True
        except Exception:
            pass
        if merged_ids_arr:
            is_merged = True

        if not is_merged:
            continue

        merged_found += 1
        rows_to_upsert.append(
            (
                int(ticket_id),
                int(merged_tickets) if merged_tickets is not None else (len(merged_ids_arr) if merged_ids_arr else None),
                str(merged_ids_raw) if merged_ids_raw is not None else None,
                merged_ids_arr,
                last_update,
            )
        )

    if cfg.dry_run:
        LOG.info("dry_run=True | processed=%s merged_found=%s upsert=%s", processed, merged_found, len(rows_to_upsert))
        return

    upserted = upsert_rows(conn, cfg.db_schema, cfg.table_name, rows_to_upsert)

    min_id = min(ticket_ids)
    new_cursor = max(control.id_final, int(min_id) - 1)

    update_control_cursor(
        conn,
        cfg.db_schema,
        cfg.control_table,
        control,
        new_id_atual_merged=new_cursor,
        ultima_data_validada=datetime.now(timezone.utc),
    )

    LOG.info(
        "scanner concluído | processed=%s merged_found=%s upserted=%s | cursor_antigo=%s cursor_novo=%s",
        processed, merged_found, upserted, cursor, new_cursor
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
