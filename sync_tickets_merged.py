#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import math
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# -----------------------------
# Config / Logging
# -----------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def _setup_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("tickets_mesclados")


log = _setup_logger()


@dataclass(frozen=True)
class Config:
    # Movidesk
    movi_base_url: str
    movi_token: str

    # DB
    db_schema: str
    table_name: str

    # Sync windows
    lookback_days: int
    window_days: int

    # Behavior
    dry_run: bool
    timeout_s: int


def load_config() -> Config:
    movi_token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVI_TOKEN") or os.getenv("MOVI_TOKEN")
    if not movi_token:
        raise RuntimeError("Falta token do Movidesk. Use MOVIDESK_TOKEN (recomendado).")

    movi_base_url = os.getenv("MOVIDESK_BASE_URL") or os.getenv("MOVI_BASE_URL") or "https://api.movidesk.com/public/v1"
    movi_base_url = movi_base_url.rstrip("/")

    db_schema = os.getenv("DB_SCHEMA", "visualizacao_resolvidos")
    table_name = os.getenv("TABLE_NAME", "tickets_mesclados")

    lookback_days = _env_int("LOOKBACK_DAYS", 30)
    window_days = _env_int("WINDOW_DAYS", 7)
    if window_days <= 0:
        window_days = 7

    dry_run = _env_bool("DRY_RUN", False)
    timeout_s = _env_int("HTTP_TIMEOUT_S", 60)

    return Config(
        movi_base_url=movi_base_url,
        movi_token=movi_token,
        db_schema=db_schema,
        table_name=table_name,
        lookback_days=lookback_days,
        window_days=window_days,
        dry_run=dry_run,
        timeout_s=timeout_s,
    )


# -----------------------------
# Postgres
# -----------------------------

def pg_connect():
    """
    Conecta usando:
      - DATABASE_URL (recomendado)
      - OU PGHOST/PGDATABASE/PGUSER/PGPASSWORD (+ PGPORT opcional)
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url and db_url.strip():
        return psycopg2.connect(db_url)

    host = os.getenv("PGHOST")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not (host and dbname and user and password):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=int(port),
    )


def _ensure_schema_and_table(conn, schema: str, table: str) -> None:
    """
    Tabela “1 linha por ticketId”, compatível com o retorno do /tickets/merged:
      - ticket_id (PK)
      - merged_tickets (int)
      - merged_tickets_ids (text)  -> ex: "123;456"
      - last_update (timestamp)
      - fetched_at (timestamptz)
    Além de fazer ALTER TABLE se faltar coluna (pra evitar erros de coluna inexistente).
    """
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                ticket_id BIGINT PRIMARY KEY,
                merged_tickets INTEGER NULL,
                merged_tickets_ids TEXT NULL,
                last_update TIMESTAMP NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        # Checar colunas existentes
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, table))
        existing = {r[0] for r in cur.fetchall()}

        def add_col(col: str, ddl_type: str):
            if col not in existing:
                log.warning('Coluna ausente "%s". Fazendo ALTER TABLE para adicionar...', col)
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN {col} {ddl_type};')
                existing.add(col)

        add_col("merged_tickets", "INTEGER NULL")
        add_col("merged_tickets_ids", "TEXT NULL")
        add_col("last_update", "TIMESTAMP NULL")
        add_col("fetched_at", "TIMESTAMPTZ NOT NULL DEFAULT now()")

    conn.commit()


def upsert_rows(conn, schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    # Monta valores
    values = []
    for r in rows:
        values.append((
            int(r["ticket_id"]),
            r.get("merged_tickets"),
            r.get("merged_tickets_ids"),
            r.get("last_update"),
            r.get("fetched_at"),
        ))

    sql = f"""
        INSERT INTO "{schema}"."{table}"
            (ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            last_update = EXCLUDED.last_update,
            fetched_at = EXCLUDED.fetched_at
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    return len(values)


# -----------------------------
# Movidesk HTTP
# -----------------------------

def _request_with_retries(
    method: str,
    url: str,
    *,
    params: Dict[str, Any],
    timeout_s: int,
    max_retries: int = 6,
) -> requests.Response:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method, url, params=params, timeout=timeout_s)
            # Retry em 429 / 5xx
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 2 ** attempt)
                log.warning("HTTP %s em %s (tentativa %s/%s). Aguardando %ss...",
                            resp.status_code, url, attempt, max_retries, wait)
                time.sleep(wait)
                continue
            return resp
        except Exception as e:
            last_exc = e
            wait = min(60, 2 ** attempt)
            log.warning("Erro de rede em %s (tentativa %s/%s): %s | aguardando %ss...",
                        url, attempt, max_retries, e, wait)
            time.sleep(wait)

    raise RuntimeError(f"Falha após retries em {url}: {last_exc}")


def _parse_last_update(s: Optional[str]) -> Optional[datetime]:
    """
    No manual costuma vir "YYYY-MM-DD HH:MM:SS" (sem TZ).
    Armazeno como timestamp (sem tz) no Postgres.
    """
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt
        except Exception:
            continue
    return None


def fetch_tickets_merged(cfg: Config, start_d: date, end_d: date) -> List[Dict[str, Any]]:
    """
    GET /tickets/merged?token=...&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&page=N

    Retorno normal (range) inclui:
      - mergedTickets: [ { ticketId, mergedTickets, mergedTicketsIds, lastUpdate, ... } ]
      - pageNumber / totalPages
    """
    url = f"{cfg.movi_base_url}/tickets/merged"

    # manual pede UTC e formato YYYY-MM-DD
    start_str = start_d.strftime("%Y-%m-%d")
    end_str = end_d.strftime("%Y-%m-%d")

    page = 1
    all_items: List[Dict[str, Any]] = []

    while True:
        params = {
            "token": cfg.movi_token,
            "startDate": start_str,
            "endDate": end_str,
            "page": page,
        }

        resp = _request_with_retries("GET", url, params=params, timeout_s=cfg.timeout_s)
        if resp.status_code == 404:
            # Esse 404 normalmente acontece quando a URL está errada (ex: faltando /public/v1)
            # ou o endpoint não está habilitado. Aqui a gente loga o URL completo.
            log.warning("Endpoint retornou 404. URL chamada: %s | params=%s | body=%s", url, {k:v for k,v in params.items() if k!="token"}, resp.text[:500])
            return []

        if resp.status_code >= 400:
            log.warning("Erro HTTP %s ao chamar %s | params=%s | body=%s", resp.status_code, url, {k:v for k,v in params.items() if k!="token"}, resp.text[:800])
            return []

        data = resp.json() if resp.content else {}
        items = data.get("mergedTickets") or []
        all_items.extend(items)

        page_number = data.get("pageNumber")
        total_pages = data.get("totalPages")

        # Se não vier paginação, encerra
        if not page_number or not total_pages:
            break

        try:
            page_number_i = int(page_number)
            total_pages_i = int(total_pages)
        except Exception:
            break

        if page_number_i >= total_pages_i:
            break

        page += 1

    return all_items


# -----------------------------
# Main sync
# -----------------------------

def daterange_windows(end_inclusive: date, lookback_days: int, window_days: int) -> List[Tuple[date, date]]:
    start = end_inclusive - timedelta(days=lookback_days)
    windows = []
    cur = start
    while cur <= end_inclusive:
        win_end = min(end_inclusive, cur + timedelta(days=window_days - 1))
        windows.append((cur, win_end))
        cur = win_end + timedelta(days=1)
    return windows


def main() -> None:
    cfg = load_config()

    # “hoje” em UTC pra bater com o manual (datas UTC)
    today_utc = datetime.now(timezone.utc).date()
    start_utc = today_utc - timedelta(days=cfg.lookback_days)

    log.info(
        "sync iniciando | schema=%s tabela=%s | intervalo=%s..%s | lookback_days=%s | window_days=%s | dry_run=%s",
        cfg.db_schema, cfg.table_name, start_utc, today_utc, cfg.lookback_days, cfg.window_days, cfg.dry_run
    )

    if cfg.dry_run:
        conn = None
    else:
        conn = pg_connect()
        _ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

    total_upsert = 0
    windows = daterange_windows(today_utc, cfg.lookback_days, cfg.window_days)

    for (w_start, w_end) in windows:
        log.info("buscando /tickets/merged | startDate=%s endDate=%s", w_start, w_end)

        items = fetch_tickets_merged(cfg, w_start, w_end)
        if not items:
            continue

        fetched_at = datetime.now(timezone.utc)

        batch: List[Dict[str, Any]] = []
        for it in items:
            # Campos típicos do retorno
            ticket_id = it.get("ticketId") or it.get("id")  # fallback defensivo
            if ticket_id is None:
                continue

            merged_tickets = it.get("mergedTickets")
            try:
                merged_tickets_i = int(merged_tickets) if merged_tickets is not None else None
            except Exception:
                merged_tickets_i = None

            merged_tickets_ids = it.get("mergedTicketsIds")
            if merged_tickets_ids is not None:
                merged_tickets_ids = str(merged_tickets_ids).strip()

            last_update = _parse_last_update(it.get("lastUpdate"))

            batch.append({
                "ticket_id": int(ticket_id),
                "merged_tickets": merged_tickets_i,
                "merged_tickets_ids": merged_tickets_ids,
                "last_update": last_update,
                "fetched_at": fetched_at,
            })

        if cfg.dry_run:
            log.info("DRY_RUN: capturados %s tickets nesta janela (não gravando no banco).", len(batch))
            continue

        total_upsert += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)
        log.info("janela %s..%s: upsert %s linhas", w_start, w_end, len(batch))

    log.info("sincronização concluída. Total upserts nesta execução: %s", total_upsert)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Falha fatal: %s", e)
        sys.exit(1)
