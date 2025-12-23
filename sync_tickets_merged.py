#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import math
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("tickets_mesclados")


# ----------------------------
# Config
# ----------------------------
@dataclass
class Config:
    api_base: str
    api_token: str

    db_schema: str
    table_name: str

    window_days: int
    days_back: int
    start_date: Optional[date]
    end_date: Optional[date]

    throttle_seconds: float
    timeout_seconds: int
    max_retries: int


def parse_date_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def env_config() -> Config:
    api_base = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").rstrip("/")
    api_token = os.getenv("MOVIDESK_API_TOKEN") or os.getenv("MOVIDESK_TOKEN") or ""
    if not api_token:
        raise RuntimeError("Falta MOVIDESK_API_TOKEN (ou MOVIDESK_TOKEN).")

    db_schema = os.getenv("DB_SCHEMA", "visualizacao_resolvidos")
    table_name = os.getenv("TABLE_NAME", "tickets_mesclados")

    window_days = int(os.getenv("WINDOW_DAYS", "7"))
    if window_days < 1:
        window_days = 1
    if window_days > 7:
        # manual diz janela máxima 7 dias
        window_days = 7

    days_back = int(os.getenv("DAYS_BACK", "30"))

    start_date = os.getenv("START_DATE", "").strip() or None
    end_date = os.getenv("END_DATE", "").strip() or None

    throttle_seconds = float(os.getenv("THROTTLE_SECONDS", "6.2"))
    timeout_seconds = int(os.getenv("HTTP_TIMEOUT", "60"))
    max_retries = int(os.getenv("MAX_RETRIES", "5"))

    return Config(
        api_base=api_base,
        api_token=api_token,
        db_schema=db_schema,
        table_name=table_name,
        window_days=window_days,
        days_back=days_back,
        start_date=parse_date_yyyy_mm_dd(start_date) if start_date else None,
        end_date=parse_date_yyyy_mm_dd(end_date) if end_date else None,
        throttle_seconds=throttle_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )


# ----------------------------
# Postgres
# ----------------------------
def pg_connect():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return psycopg2.connect(database_url)

    host = os.getenv("PGHOST", "").strip()
    db = os.getenv("PGDATABASE", "").strip()
    user = os.getenv("PGUSER", "").strip()
    pwd = os.getenv("PGPASSWORD", "").strip()
    port = os.getenv("PGPORT", "5432").strip()

    if not (host and db and user and pwd):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(host=host, dbname=db, user=user, password=pwd, port=port)


def ensure_schema_and_table(conn, schema: str, table: str) -> None:
    """
    Cria schema/tabela e garante colunas necessárias.
    Estrutura alvo:
      ticket_id bigint
      merged_ticket_id bigint
      merged_tickets integer
      merged_tickets_ids text
      last_update timestamptz
      fetched_at timestamptz
      raw jsonb
    """
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
            ticket_id        BIGINT,
            merged_ticket_id BIGINT,
            merged_tickets   INTEGER,
            merged_tickets_ids TEXT,
            last_update      TIMESTAMPTZ,
            fetched_at       TIMESTAMPTZ,
            raw             JSONB
        );
        """)

        # add missing columns (para tabelas antigas)
        required = {
            "ticket_id": 'BIGINT',
            "merged_ticket_id": 'BIGINT',
            "merged_tickets": 'INTEGER',
            "merged_tickets_ids": 'TEXT',
            "last_update": 'TIMESTAMPTZ',
            "fetched_at": 'TIMESTAMPTZ',
            "raw": 'JSONB',
        }
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, table))
        existing = {r[0] for r in cur.fetchall()}

        for col, coltype in required.items():
            if col not in existing:
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col}" {coltype};')

        # índice único para upsert
        # (UNIQUE permite múltiplos NULLs, então não “quebra” tabela antiga)
        cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s AND indexname = %s
            ) THEN
                EXECUTE format('CREATE UNIQUE INDEX %I ON "{schema}"."{table}" (ticket_id, merged_ticket_id);', %s);
            END IF;
        END$$;
        """, (schema, table, f"{table}_ticket_merged_uidx", f"{table}_ticket_merged_uidx"))

    conn.commit()


def get_max_last_update_date(conn, schema: str, table: str) -> Optional[date]:
    with conn.cursor() as cur:
        try:
            cur.execute(f'SELECT MAX(last_update) FROM "{schema}"."{table}";')
            val = cur.fetchone()[0]
            if not val:
                return None
            if isinstance(val, datetime):
                return val.date()
            return None
        except Exception:
            conn.rollback()
            return None


def upsert_rows(conn, schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = f"""
    INSERT INTO "{schema}"."{table}"
        (ticket_id, merged_ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at, raw)
    VALUES %s
    ON CONFLICT (ticket_id, merged_ticket_id)
    DO UPDATE SET
        merged_tickets = EXCLUDED.merged_tickets,
        merged_tickets_ids = EXCLUDED.merged_tickets_ids,
        last_update = EXCLUDED.last_update,
        fetched_at = EXCLUDED.fetched_at,
        raw = EXCLUDED.raw
    ;
    """

    values = []
    for r in rows:
        values.append((
            r.get("ticket_id"),
            r.get("merged_ticket_id"),
            r.get("merged_tickets"),
            r.get("merged_tickets_ids"),
            r.get("last_update"),
            r.get("fetched_at"),
            psycopg2.extras.Json(r.get("raw")),
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    return len(rows)


# ----------------------------
# Movidesk API
# ----------------------------
def http_get_json(url: str, params: Dict[str, Any], timeout: int, max_retries: int) -> Any:
    """
    GET com retry simples (inclui 429).
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 65
                log.warning("HTTP 429 (rate limit). Dormindo %ss e tentando novamente (tentativa %s/%s).", sleep_s, attempt, max_retries)
                time.sleep(sleep_s)
                continue

            if resp.status_code == 404:
                # em geral: sem dados/rota; trate como vazio
                log.warning("HTTP 404 no endpoint (tratando como vazio): %s", resp.url)
                return []

            resp.raise_for_status()
            if not resp.text.strip():
                return []
            return resp.json()
        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            sleep_s = min(2 ** attempt, 30)
            log.warning("Erro HTTP (%s). Retry em %ss (tentativa %s/%s).", str(e), sleep_s, attempt, max_retries)
            time.sleep(sleep_s)

    return []


def fetch_merged_window(cfg: Config, start_d: date, end_d: date) -> List[Dict[str, Any]]:
    # manual: /tickets/merged?token=TOKEN&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD:contentReference[oaicite:7]{index=7}
    url = f"{cfg.api_base}/tickets/merged"
    params = {
        "token": cfg.api_token,
        "startDate": start_d.isoformat(),
        "endDate": end_d.isoformat(),
    }
    data = http_get_json(url, params=params, timeout=cfg.timeout_seconds, max_retries=cfg.max_retries)
    if not isinstance(data, list):
        log.warning("Resposta inesperada (não-list) em %s..%s: %s", start_d, end_d, type(data))
        return []
    return data


def parse_last_update(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    # tenta ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def split_merged_ids(v: Any) -> List[int]:
    if v is None:
        return []
    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(";") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            continue
    return out


def normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Esperado pelo manual: ticketId, mergedTickets, mergedTicketsIds, lastUpdate:contentReference[oaicite:8]{index=8}
    """
    fetched_at = datetime.now(timezone.utc)
    out: List[Dict[str, Any]] = []

    for it in items:
        if not isinstance(it, dict):
            continue

        ticket_id = it.get("ticketId")
        merged_tickets = it.get("mergedTickets")
        merged_ids_raw = it.get("mergedTicketsIds")
        last_update_raw = it.get("lastUpdate")

        try:
            ticket_id_int = int(ticket_id)
        except Exception:
            log.warning("Item ignorado (ticketId inválido): %s", it)
            continue

        last_update = parse_last_update(last_update_raw)
        merged_ids = split_merged_ids(merged_ids_raw)

        # Se mergedTickets > 0 mas mergedTicketsIds vier vazio, não dá pra explodir em pares
        if (merged_tickets not in (None, "", 0, "0")) and not merged_ids:
            log.warning("Item com mergedTickets>0 mas sem mergedTicketsIds: %s", it)
            continue

        # 1 linha por par (ticket_id, merged_ticket_id)
        for mid in merged_ids:
            out.append({
                "ticket_id": ticket_id_int,
                "merged_ticket_id": mid,
                "merged_tickets": int(merged_tickets) if str(merged_tickets).strip().isdigit() else None,
                "merged_tickets_ids": str(merged_ids_raw) if merged_ids_raw is not None else None,
                "last_update": last_update,
                "fetched_at": fetched_at,
                "raw": it,
            })

    return out


def daterange_windows(start_d: date, end_d: date, window_days: int) -> Iterable[Tuple[date, date]]:
    cur = start_d
    while cur <= end_d:
        w_end = min(end_d, cur + timedelta(days=window_days - 1))
        yield (cur, w_end)
        cur = w_end + timedelta(days=1)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    cfg = env_config()

    conn = pg_connect()
    try:
        ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

        today = date.today()
        end_d = cfg.end_date or today
        if cfg.start_date:
            start_d = cfg.start_date
        else:
            max_d = get_max_last_update_date(conn, cfg.db_schema, cfg.table_name)
            if max_d:
                # safety: volta 1 dia
                start_d = max(max_d - timedelta(days=1), end_d - timedelta(days=cfg.days_back))
            else:
                start_d = end_d - timedelta(days=cfg.days_back)

        log.info(
            "tickets_mesclados: sync iniciando | schema=%s tabela=%s | janela=%s..%s (window_days=%s) | api_base=%s",
            cfg.db_schema, cfg.table_name, start_d.isoformat(), end_d.isoformat(), cfg.window_days, cfg.api_base
        )

        total_items = 0
        total_rows = 0
        for w_start, w_end in daterange_windows(start_d, end_d, cfg.window_days):
            log.info("tickets_mesclados: buscando /tickets/merged startDate=%s endDate=%s", w_start.isoformat(), w_end.isoformat())
            items = fetch_merged_window(cfg, w_start, w_end)
            total_items += len(items)

            rows = normalize_items(items)
            if rows:
                total_rows += upsert_rows(conn, cfg.db_schema, cfg.table_name, rows)

            # throttle para respeitar limites (ex.: 10 req/min)
            time.sleep(cfg.throttle_seconds)

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{cfg.db_schema}"."{cfg.table_name}";')
            total_table = cur.fetchone()[0]

        log.info("tickets_mesclados: sync concluído | itens_api=%s rows_upsert=%s total_tabela=%s", total_items, total_rows, total_table)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
