#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import math
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# -----------------------------
# Logging
# -----------------------------
LOG = logging.getLogger("tickets_mesclados")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    movidesk_token: str
    base_url: str = "https://api.movidesk.com/public/v1"
    timeout_sec: int = 60
    retries: int = 3
    retry_sleep_sec: float = 2.0

    db_schema: str = "visualizacao_resolvidos"
    table_name: str = "tickets_mesclados"

    lookback_days: int = 30
    window_days: int = 7

    dry_run: bool = False


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Movidesk merged tickets into Postgres")
    p.add_argument("--schema", default=os.getenv("DB_SCHEMA", "visualizacao_resolvidos"))
    p.add_argument("--table", default=os.getenv("DB_TABLE", "tickets_mesclados"))
    p.add_argument("--lookback-days", type=int, default=getenv_int("LOOKBACK_DAYS", 30))
    p.add_argument("--window-days", type=int, default=getenv_int("WINDOW_DAYS", 7))
    p.add_argument("--dry-run", action="store_true", default=getenv_bool("DRY_RUN", False))
    return p.parse_args()


# -----------------------------
# Postgres
# -----------------------------
def pg_connect() -> psycopg2.extensions.connection:
    """
    Conecta via DATABASE_URL (preferencial) ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD/PGPORT.
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
        raise RuntimeError(
            "Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

    return psycopg2.connect(
        host=host, dbname=dbname, user=user, password=password, port=port
    )


def ensure_schema_and_table(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    """
    Cria schema/tabela se não existirem e garante colunas necessárias.
    """
    ddl_schema = f'CREATE SCHEMA IF NOT EXISTS "{schema}";'

    ddl_table = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
        ticket_id BIGINT NOT NULL,
        merged_tickets INTEGER NULL,
        merged_tickets_ids TEXT NULL,
        last_update TIMESTAMPTZ NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    # Garante unique para ON CONFLICT
    ddl_uniq = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS "{table}_ticket_id_ux"
    ON "{schema}"."{table}" (ticket_id);
    """

    # Garante colunas (caso a tabela exista antiga)
    needed_cols = {
        "ticket_id": 'ALTER TABLE "{schema}"."{table}" ADD COLUMN ticket_id BIGINT;',
        "merged_tickets": 'ALTER TABLE "{schema}"."{table}" ADD COLUMN merged_tickets INTEGER;',
        "merged_tickets_ids": 'ALTER TABLE "{schema}"."{table}" ADD COLUMN merged_tickets_ids TEXT;',
        "last_update": 'ALTER TABLE "{schema}"."{table}" ADD COLUMN last_update TIMESTAMPTZ;',
        "fetched_at": 'ALTER TABLE "{schema}"."{table}" ADD COLUMN fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW();',
    }

    with conn.cursor() as cur:
        cur.execute(ddl_schema)
        cur.execute(ddl_table)
        conn.commit()

        # checa colunas existentes
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        for col, ddl_tpl in needed_cols.items():
            if col not in existing:
                ddl = ddl_tpl.format(schema=schema, table=table)
                LOG.warning("Tabela sem coluna %s. Executando: %s", col, ddl.strip())
                cur.execute(ddl)
                conn.commit()

        cur.execute(ddl_uniq)
        conn.commit()


def upsert_rows(
    conn: psycopg2.extensions.connection, schema: str, table: str, rows: List[Dict[str, Any]]
) -> int:
    """
    Upsert por ticket_id.
    Retorna quantidade de linhas enviadas (não necessariamente rowcount real).
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO "{schema}"."{table}"
        (ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at)
    VALUES %s
    ON CONFLICT (ticket_id) DO UPDATE SET
        merged_tickets = EXCLUDED.merged_tickets,
        merged_tickets_ids = EXCLUDED.merged_tickets_ids,
        last_update = EXCLUDED.last_update,
        fetched_at = EXCLUDED.fetched_at
    ;
    """

    values = []
    now = datetime.now(timezone.utc)
    for r in rows:
        values.append(
            (
                int(r["ticket_id"]),
                r.get("merged_tickets"),
                r.get("merged_tickets_ids"),
                r.get("last_update"),
                now,
            )
        )

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
            conn.commit()
            return len(values)
        except Exception:
            conn.rollback()
            raise


# -----------------------------
# Movidesk API helpers
# -----------------------------
def http_get_json(cfg: Config, path: str, params: Dict[str, Any]) -> Any:
    url = cfg.base_url.rstrip("/") + "/" + path.lstrip("/")

    # token é via querystring no Movidesk
    params = dict(params)
    params["token"] = cfg.movidesk_token

    last_err = None
    for attempt in range(1, cfg.retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=cfg.timeout_sec)
            if resp.status_code == 404:
                # endpoint não existe nessa conta/cluster
                raise requests.HTTPError("404 Not Found", response=resp)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < cfg.retries:
                time.sleep(cfg.retry_sleep_sec * attempt)
            else:
                raise

    raise last_err  # pragma: no cover


def parse_page_number(page_number: Optional[str]) -> Tuple[int, int]:
    """
    pageNumber pode vir como "1 of 333" (manual).
    Retorna (current, total). Se não der, retorna (1,1).
    """
    if not page_number:
        return (1, 1)
    s = str(page_number).strip()
    if "of" in s:
        parts = [p.strip() for p in s.split("of")]
        try:
            cur = int(parts[0])
            tot = int(parts[1])
            return (cur, tot)
        except Exception:
            return (1, 1)
    return (1, 1)


def normalize_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Aceita variações:
      - ticketId (manual /tickets/merged)
      - id (algumas respostas antigas)
    mergedTicketsIds geralmente é string com ';'
    lastUpdate pode vir como string "YYYY-MM-DD HH:MM:SS"
    """
    if not isinstance(item, dict):
        return None

    ticket_id = item.get("ticketId", item.get("id"))
    if ticket_id is None:
        return None

    merged_tickets = item.get("mergedTickets")
    merged_tickets_ids = item.get("mergedTicketsIds")

    # normalize ints/strings
    try:
        if merged_tickets is not None and merged_tickets != "":
            merged_tickets = int(merged_tickets)
        else:
            merged_tickets = None
    except Exception:
        merged_tickets = None

    if merged_tickets_ids is not None:
        merged_tickets_ids = str(merged_tickets_ids).strip()
        if merged_tickets_ids == "":
            merged_tickets_ids = None

    # lastUpdate -> timestamptz
    last_update = None
    lu = item.get("lastUpdate")
    if lu:
        s = str(lu).strip()
        # tenta formatos comuns: "YYYY-MM-DD HH:MM:SS" ou ISO
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                last_update = dt
                break
            except Exception:
                continue

    return {
        "ticket_id": int(ticket_id),
        "merged_tickets": merged_tickets,
        "merged_tickets_ids": merged_tickets_ids,
        "last_update": last_update,
    }


def extract_items_from_response(data: Any) -> List[Dict[str, Any]]:
    """
    No manual do /tickets/merged:
      { startDate, endDate, count, pageNumber, mergedTickets: [...] }

    Mas em alguns ambientes pode vir direto como lista.
    """
    if isinstance(data, dict):
        if "mergedTickets" in data and isinstance(data["mergedTickets"], list):
            return data["mergedTickets"]
        # fallback: alguns retornos podem vir com "value"
        if "value" in data and isinstance(data["value"], list):
            return data["value"]
        # ou caso venham itens num formato inesperado:
        return []
    if isinstance(data, list):
        return data
    return []


def fetch_merged_window(cfg: Config, start_d: date, end_d: date) -> List[Dict[str, Any]]:
    """
    Busca todos os merged tickets no intervalo (startDate/endDate) paginando.
    No manual, startDate/endDate são datas.
    """
    all_items: List[Dict[str, Any]] = []

    # Manual aceita datas (YYYY-MM-DD). Vamos seguir isso para evitar 404/400 por formato.
    start_s = start_d.isoformat()
    end_s = end_d.isoformat()

    page = 1
    total_pages = 1

    while page <= total_pages:
        LOG.info(
            "buscando /tickets/merged | page=%s | startDate=%s endDate=%s",
            page, start_s, end_s
        )
        data = http_get_json(
            cfg,
            "/tickets/merged",
            {"startDate": start_s, "endDate": end_s, "page": page},
        )

        # total pages (se vier)
        if isinstance(data, dict):
            _, total_pages = parse_page_number(data.get("pageNumber"))
        else:
            total_pages = 1

        items = extract_items_from_response(data)
        all_items.extend(items)
        page += 1

    return all_items


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    args = parse_args()

    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIESK_TOKEN") or os.getenv("TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN nas variáveis de ambiente (GitHub Secrets).")

    cfg = Config(
        movidesk_token=token,
        db_schema=args.schema,
        table_name=args.table,
        lookback_days=args.lookback_days,
        window_days=args.window_days,
        dry_run=args.dry_run,
    )

    today_utc = datetime.now(timezone.utc).date()
    start_all = today_utc - timedelta(days=cfg.lookback_days)
    end_all = today_utc

    LOG.info(
        "sync iniciando | schema=%s tabela=%s | intervalo=%s..%s | lookback_days=%s | window_days=%s | dry_run=%s",
        cfg.db_schema, cfg.table_name, start_all.isoformat(), end_all.isoformat(),
        cfg.lookback_days, cfg.window_days, cfg.dry_run
    )

    conn = pg_connect()
    try:
        ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

        total_api = 0
        total_upsert_sent = 0

        cur_start = start_all
        while cur_start <= end_all:
            cur_end = min(cur_start + timedelta(days=cfg.window_days - 1), end_all)

            raw_items = fetch_merged_window(cfg, cur_start, cur_end)
            total_api += len(raw_items)

            normalized: List[Dict[str, Any]] = []
            for it in raw_items:
                n = normalize_item(it)
                if not n:
                    # loga um exemplo sem explodir o log
                    try:
                        LOG.warning("item inesperado em /tickets/merged (descartado): %s", json.dumps(it)[:500])
                    except Exception:
                        LOG.warning("item inesperado em /tickets/merged (descartado).")
                    continue
                normalized.append(n)

            # opcional: só grava quem tem merge de fato
            normalized = [
                r for r in normalized
                if (r.get("merged_tickets") and r["merged_tickets"] > 0) or (r.get("merged_tickets_ids"))
            ]

            if cfg.dry_run:
                LOG.info("dry_run: janela %s..%s teria %s registros para upsert", cur_start, cur_end, len(normalized))
            else:
                up = upsert_rows(conn, cfg.db_schema, cfg.table_name, normalized)
                total_upsert_sent += up

            cur_start = cur_end + timedelta(days=1)

        # total na tabela
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{cfg.db_schema}"."{cfg.table_name}"')
            total_table = cur.fetchone()[0]

        LOG.info(
            "sincronização concluída | itens_api=%s | linhas_upsert_enviadas=%s | total_tabela=%s",
            total_api, total_upsert_sent, total_table
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
