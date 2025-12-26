#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scanner incremental de tickets mesclados (Movidesk).

- Lê um scan aberto na tabela de controle (range_scan_control).
- Seleciona tickets em tickets_resolvidos_detail dentro do intervalo data_inicio..data_fim.
- Processa em ordem decrescente, a partir de id_atual_merged (ponteiro), em lotes (LIMIT).
- Para cada ticket_id, consulta a API /tickets/merged?id=<ticket_id>.
- Se o ticket tiver mescla (mergedTickets > 0), faz upsert em tickets_mesclados.
- Atualiza id_atual_merged e ultima_data_validada a cada execução.

ENV (mesmos nomes do workflow original)
- Token Movidesk: MOVIDESK_TOKEN (fallback MOVI_TOKEN)
- Postgres DSN: NEON_DSN (fallback DATABASE_URL) ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD

ENV opcionais
- DB_SCHEMA (default: visualizacao_resolvidos)
- TABLE_NAME (default: tickets_mesclados)
- CONTROL_TABLE (default: range_scan_control)
- RESOLVED_TABLE (default: tickets_resolvidos_detail)
- RESOLVED_ID_COL (override do nome da coluna ID)
- RESOLVED_DATE_COL (override do nome da coluna de data/fechamento)
- LIMIT (default: 80)
- RPM (default: 9) => throttle = 60/rpm entre requisições
- DRY_RUN (default: false) => não escreve no banco
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests

BASE_URL = "https://api.movidesk.com/public/v1"


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"ENV {name} inválida (esperado int): {v!r}")


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        raise RuntimeError(f"ENV {name} inválida (esperado float): {v!r}")


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    v = v.strip().lower()
    if v in ("1", "true", "t", "yes", "y", "on"):
        return True
    if v in ("0", "false", "f", "no", "n", "off"):
        return False
    raise RuntimeError(f"ENV {name} inválida (esperado bool): {v!r}")


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("range_scan")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger


log = setup_logger()


@dataclass
class Settings:
    movi_token: str
    db_schema: str
    table_name: str
    control_table: str
    resolved_table: str
    resolved_id_col: Optional[str]
    resolved_date_col: Optional[str]
    limit: int
    rpm: float
    dry_run: bool

    @property
    def throttle_seconds(self) -> float:
        if self.rpm <= 0:
            return 0.0
        return 60.0 / float(self.rpm)


def load_settings() -> Settings:
    token = env_str("MOVIDESK_TOKEN") or env_str("MOVI_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVI_TOKEN) - token da API Movidesk.")

    return Settings(
        movi_token=token,
        db_schema=env_str("DB_SCHEMA", "visualizacao_resolvidos") or "visualizacao_resolvidos",
        table_name=env_str("TABLE_NAME", "tickets_mesclados") or "tickets_mesclados",
        control_table=env_str("CONTROL_TABLE", "range_scan_control") or "range_scan_control",
        resolved_table=env_str("RESOLVED_TABLE", "tickets_resolvidos_detail") or "tickets_resolvidos_detail",
        resolved_id_col=env_str("RESOLVED_ID_COL"),
        resolved_date_col=env_str("RESOLVED_DATE_COL"),
        limit=max(1, env_int("LIMIT", 80)),
        rpm=env_float("RPM", 9.0),
        dry_run=env_bool("DRY_RUN", False),
    )


def pg_connect() -> "psycopg2.extensions.connection":
    dsn = env_str("DATABASE_URL") or env_str("NEON_DSN")
    if dsn:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        return conn

    host = env_str("PGHOST")
    dbname = env_str("PGDATABASE")
    user = env_str("PGUSER")
    password = env_str("PGPASSWORD")
    port = env_str("PGPORT", "5432")
    sslmode = env_str("PGSSLMODE")

    if not (host and dbname and user and password):
        raise RuntimeError(
            "Faltam variáveis de Postgres. Use NEON_DSN/DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

    kwargs: Dict[str, Any] = dict(host=host, dbname=dbname, user=user, password=password, port=port)
    if sslmode:
        kwargs["sslmode"] = sslmode

    conn = psycopg2.connect(**kwargs)
    conn.autocommit = True
    return conn


def ensure_schema_and_tables(conn, schema: str, tickets_table: str, control_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{tickets_table}" (
                ticket_id BIGINT PRIMARY KEY,
                merged_tickets INTEGER,
                merged_tickets_ids TEXT,
                merged_ticket_ids_arr BIGINT[],
                last_update TIMESTAMP,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{control_table}" (
                id BIGSERIAL PRIMARY KEY,
                data_inicio TIMESTAMPTZ NOT NULL,
                data_fim TIMESTAMPTZ NOT NULL,
                ultima_data_validada TIMESTAMPTZ,
                id_inicial BIGINT,
                id_final BIGINT,
                id_atual BIGINT,
                id_atual_merged BIGINT,
                concluido_em TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        for col, ddl in [
            ("ultima_data_validada", "TIMESTAMPTZ"),
            ("id_inicial", "BIGINT"),
            ("id_final", "BIGINT"),
            ("id_atual", "BIGINT"),
            ("id_atual_merged", "BIGINT"),
            ("concluido_em", "TIMESTAMPTZ"),
            ("created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
        ]:
            cur.execute(f'ALTER TABLE "{schema}"."{control_table}" ADD COLUMN IF NOT EXISTS {col} {ddl};')


def get_table_columns(conn, schema: str, table: str) -> List[Tuple[str, str, str]]:
    sql = """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def guess_id_col(cols: List[Tuple[str, str, str]]) -> Optional[str]:
    preferred = ["ticket_id", "ticketid", "id"]
    colnames = [c[0] for c in cols]
    lower_map = {c.lower(): c for c in colnames}

    for p in preferred:
        if p in lower_map:
            return lower_map[p]

    for name, data_type, _udt in cols:
        n = name.lower()
        if "ticket" in n and data_type in ("integer", "bigint", "numeric"):
            return name

    for name, data_type, _udt in cols:
        if data_type in ("integer", "bigint", "numeric"):
            return name

    return None


def guess_date_col(cols: List[Tuple[str, str, str]]) -> Optional[str]:
    preferred = [
        "resolved_at", "resolved_date", "resolveddate",
        "closed_at", "closed_date", "closeddate",
        "finished_at", "finished_date",
        "completed_at", "completed_date",
        "updated_at", "last_update", "lastupdate",
        "created_at", "created_date",
        "data_resolucao", "data_resolvido", "data_resolvido_em",
        "data_fechamento", "data_conclusao", "fechado_em", "resolvido_em",
        "data_finalizacao", "data_encerramento",
    ]
    colnames = [c[0] for c in cols]
    lower_map = {c.lower(): c for c in colnames}

    for p in preferred:
        if p in lower_map:
            return lower_map[p]

    for name, data_type, _udt in cols:
        n = name.lower()
        if data_type in ("timestamp with time zone", "timestamp without time zone", "date"):
            if any(k in n for k in ("resol", "fech", "close", "finish", "complete")):
                return name

    for name, data_type, _udt in cols:
        if data_type in ("timestamp with time zone", "timestamp without time zone", "date"):
            return name

    return None


@dataclass
class ControlRow:
    id: int
    data_inicio: dt.datetime
    data_fim: dt.datetime
    ultima_data_validada: Optional[dt.datetime]
    id_inicial: Optional[int]
    id_final: Optional[int]
    id_atual_merged: Optional[int]
    concluido_em: Optional[dt.datetime]


def read_open_control(conn, schema: str, control_table: str) -> Optional[ControlRow]:
    sql = f"""
        SELECT id, data_inicio, data_fim, ultima_data_validada,
               id_inicial, id_final, id_atual_merged, concluido_em
        FROM "{schema}"."{control_table}"
        WHERE concluido_em IS NULL
        ORDER BY data_inicio DESC, id DESC
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return None
        return ControlRow(
            id=int(row[0]),
            data_inicio=row[1],
            data_fim=row[2],
            ultima_data_validada=row[3],
            id_inicial=row[4],
            id_final=row[5],
            id_atual_merged=row[6],
            concluido_em=row[7],
        )


def update_control_progress(
    conn,
    schema: str,
    control_table: str,
    control_id: int,
    *,
    id_inicial: Optional[int] = None,
    id_final: Optional[int] = None,
    id_atual_merged: Optional[int] = None,
    ultima_data_validada: Optional[dt.datetime] = None,
    concluido_em: Optional[dt.datetime] = None,
) -> None:
    sets = []
    params: List[Any] = []

    if id_inicial is not None:
        sets.append("id_inicial = %s")
        params.append(id_inicial)
    if id_final is not None:
        sets.append("id_final = %s")
        params.append(id_final)
    if id_atual_merged is not None:
        sets.append("id_atual_merged = %s")
        params.append(id_atual_merged)
    if ultima_data_validada is not None:
        sets.append("ultima_data_validada = %s")
        params.append(ultima_data_validada)
    if concluido_em is not None:
        sets.append("concluido_em = %s")
        params.append(concluido_em)

    sets.append("updated_at = NOW()")

    sql = f"""
        UPDATE "{schema}"."{control_table}"
        SET {", ".join(sets)}
        WHERE id = %s;
    """
    params.append(control_id)

    with conn.cursor() as cur:
        cur.execute(sql, params)


class MovideskClient:
    def __init__(self, token: str, rpm: float):
        self.token = token
        self.session = requests.Session()
        self.throttle = 0.0 if rpm <= 0 else 60.0 / float(rpm)
        self._last_call_ts = 0.0

    def _sleep_throttle(self) -> None:
        if self.throttle <= 0:
            return
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.throttle:
            time.sleep(self.throttle - elapsed)
        self._last_call_ts = time.time()

    def get_merged_by_ticket(self, ticket_id: int, timeout: int = 30) -> Optional[Dict[str, Any]]:
        self._sleep_throttle()
        url = f"{BASE_URL}/tickets/merged"
        params = {"token": self.token, "id": str(ticket_id)}

        for attempt in range(1, 4):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(1.5 * attempt)
                continue

            if resp.status_code == 200:
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    log.warning("JSON inválido para ticket %s: %s", ticket_id, resp.text[:200])
                    return None

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == 3:
                    log.warning("API falhou (%s) para ticket %s: %s", resp.status_code, ticket_id, resp.text[:200])
                    return None
                time.sleep(2.0 * attempt)
                continue

            if resp.status_code == 404:
                return None

            log.warning("API retornou %s para ticket %s: %s", resp.status_code, ticket_id, resp.text[:200])
            return None

        return None


def parse_last_update(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def upsert_merged_rows(
    conn,
    schema: str,
    table: str,
    rows: Sequence[Tuple[int, int, Optional[str], Optional[List[int]], Optional[dt.datetime], dt.datetime]],
) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO "{schema}"."{table}"
            (ticket_id, merged_tickets, merged_tickets_ids, merged_ticket_ids_arr, last_update, synced_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            merged_ticket_ids_arr = EXCLUDED.merged_ticket_ids_arr,
            last_update = EXCLUDED.last_update,
            synced_at = EXCLUDED.synced_at;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def compute_id_bounds(
    conn,
    schema: str,
    resolved_table: str,
    id_col: str,
    date_col: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> Tuple[Optional[int], Optional[int]]:
    sql = f"""
        SELECT MIN("{id_col}")::BIGINT, MAX("{id_col}")::BIGINT
        FROM "{schema}"."{resolved_table}"
        WHERE "{date_col}" >= %s AND "{date_col}" <= %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_dt, end_dt))
        mn, mx = cur.fetchone()
        return (mn, mx)


def fetch_candidate_batch(
    conn,
    schema: str,
    resolved_table: str,
    id_col: str,
    date_col: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    cursor_id: int,
    limit: int,
) -> List[Tuple[int, Optional[dt.datetime]]]:
    sql = f"""
        SELECT "{id_col}"::BIGINT, "{date_col}"
        FROM "{schema}"."{resolved_table}"
        WHERE "{date_col}" >= %s
          AND "{date_col}" <= %s
          AND "{id_col}" <= %s
        ORDER BY "{id_col}" DESC
        LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_dt, end_dt, cursor_id, limit))
        return [(int(r[0]), r[1]) for r in cur.fetchall()]


def existing_ticket_ids(conn, schema: str, table: str, ids: Sequence[int]) -> set:
    if not ids:
        return set()
    sql = f'SELECT ticket_id FROM "{schema}"."{table}" WHERE ticket_id = ANY(%s);'
    with conn.cursor() as cur:
        cur.execute(sql, (list(ids),))
        return {int(r[0]) for r in cur.fetchall()}


def main() -> None:
    cfg = load_settings()
    conn = pg_connect()

    ensure_schema_and_tables(conn, cfg.db_schema, cfg.table_name, cfg.control_table)

    control = read_open_control(conn, cfg.db_schema, cfg.control_table)
    if not control:
        log.info("nenhuma linha aberta em %s.%s -> encerrando OK", cfg.db_schema, cfg.control_table)
        return

    cols = get_table_columns(conn, cfg.db_schema, cfg.resolved_table)
    if not cols:
        raise RuntimeError(f"Tabela {cfg.db_schema}.{cfg.resolved_table} não existe ou não tem colunas visíveis.")

    id_col = cfg.resolved_id_col or guess_id_col(cols)
    date_col = cfg.resolved_date_col or guess_date_col(cols)
    if not id_col:
        raise RuntimeError("Não consegui identificar a coluna ID da tabela de resolvidos. Defina RESOLVED_ID_COL.")
    if not date_col:
        raise RuntimeError("Não consegui identificar a coluna de data da tabela de resolvidos. Defina RESOLVED_DATE_COL.")

    start_dt = control.data_inicio
    end_dt = control.data_fim

    mn_id, mx_id = compute_id_bounds(conn, cfg.db_schema, cfg.resolved_table, id_col, date_col, start_dt, end_dt)

    if mn_id is None or mx_id is None:
        log.info(
            "nenhum ticket em %s..%s na tabela %s.%s -> marcando scan como concluído",
            start_dt.date(), end_dt.date(), cfg.db_schema, cfg.resolved_table
        )
        if not cfg.dry_run:
            update_control_progress(
                conn, cfg.db_schema, cfg.control_table, control.id,
                ultima_data_validada=end_dt,
                concluido_em=dt.datetime.utcnow(),
            )
        return

    if not cfg.dry_run and (control.id_inicial is None or control.id_final is None):
        update_control_progress(
            conn, cfg.db_schema, cfg.control_table, control.id,
            id_inicial=int(mx_id),
            id_final=int(mn_id),
        )

    cursor_id = control.id_atual_merged if control.id_atual_merged is not None else int(mx_id)
    cursor_id = min(cursor_id, int(mx_id))

    log.info(
        "scanner iniciando | schema=%s tabela=%s control=%s | intervalo=%s..%s | id_range=%s..%s | cursor=%s | limit=%s | rpm=%.2f | throttle=%.2fs | dry_run=%s",
        cfg.db_schema, cfg.table_name, cfg.control_table,
        start_dt.date(), end_dt.date(),
        int(mx_id), int(mn_id),
        cursor_id, cfg.limit, cfg.rpm, cfg.throttle_seconds, cfg.dry_run,
    )

    batch = fetch_candidate_batch(
        conn, cfg.db_schema, cfg.resolved_table, id_col, date_col,
        start_dt, end_dt, cursor_id, cfg.limit
    )
    if not batch:
        log.info("nenhum candidato restante (cursor=%s). Finalizando scan.", cursor_id)
        if not cfg.dry_run:
            update_control_progress(
                conn, cfg.db_schema, cfg.control_table, control.id,
                id_atual_merged=int(mn_id) - 1,
                ultima_data_validada=end_dt,
                concluido_em=dt.datetime.utcnow(),
            )
        return

    ids = [tid for tid, _dtv in batch]
    exists = existing_ticket_ids(conn, cfg.db_schema, cfg.table_name, ids)

    client = MovideskClient(cfg.movi_token, cfg.rpm)
    now_ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    upsert_rows: List[Tuple[int, int, Optional[str], Optional[List[int]], Optional[dt.datetime], dt.datetime]] = []
    checked = 0
    merged_found = 0

    for tid, _resolved_dt in batch:
        checked += 1
        data = client.get_merged_by_ticket(tid)
        if not data:
            continue

        try:
            merged_cnt = int(str(data.get("mergedTickets") or "0"))
        except ValueError:
            merged_cnt = 0

        merged_ids_str = data.get("mergedTicketsIds")
        if merged_cnt <= 0 or not merged_ids_str:
            continue

        merged_ids: List[int] = []
        for part in str(merged_ids_str).split(";"):
            part = part.strip()
            if not part:
                continue
            try:
                merged_ids.append(int(part))
            except ValueError:
                continue

        last_update = parse_last_update(data.get("lastUpdate"))
        upsert_rows.append((tid, merged_cnt, str(merged_ids_str), merged_ids or None, last_update, now_ts))
        merged_found += 1

        if tid in exists:
            log.info("ticket %s é mesclado (já existia na tabela) -> upsert", tid)
        else:
            log.info("ticket %s é mesclado (novo) -> inserir/upsert", tid)

    upserted = 0
    if upsert_rows and not cfg.dry_run:
        upserted = upsert_merged_rows(conn, cfg.db_schema, cfg.table_name, upsert_rows)

    last_id = batch[-1][0]
    last_dt = batch[-1][1]
    next_cursor = int(last_id) - 1

    if not cfg.dry_run:
        update_control_progress(
            conn, cfg.db_schema, cfg.control_table, control.id,
            id_atual_merged=next_cursor,
            ultima_data_validada=last_dt or control.ultima_data_validada,
        )

    log.info(
        "scanner concluído | batch=%s checked=%s merged_found=%s upserted=%s | next_cursor=%s",
        len(batch), checked, merged_found, upserted, next_cursor
    )


if __name__ == "__main__":
    main()
