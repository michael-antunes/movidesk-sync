#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza "tickets mesclados" (Tickets_Merged) do Movidesk para Postgres.

De acordo com o manual "API do Movidesk - Tickets_Merged":
- Endpoint: GET /tickets/merged?token=TOKEN&startDate=DATA_INICIO&endDate=DATA_FIM&page=NUMERO
- startDate/endDate em UTC no formato: YYYY-MM-DD HH:MM:SS
- Resposta (busca por período):
    {
      "pageNumber": "1 of 333",
      "mergedTickets": [
        {"ticketId":"...", "mergedTickets":"...", "mergedTicketsIds":"id;id;id", "lastUpdate":"YYYY-MM-DD HH:MM:SS"},
        ...
      ]
    }

Tabela destino (default):
  schema: visualizacao_resolvidos
  table : tickets_mesclados

Colunas (criadas/ajustadas automaticamente):
  - ticket_id                BIGINT PRIMARY KEY
  - merged_tickets           INTEGER
  - merged_tickets_ids       TEXT
  - merged_ticket_ids_arr    BIGINT[]
  - last_update              TIMESTAMPTZ
  - synced_at                TIMESTAMPTZ DEFAULT now()

Env vars:
  Movidesk:
    - MOVIDESK_TOKEN (obrigatório)
    - MOVIDESK_BASE_URL (opcional; default https://api.movidesk.com/public/v1)

  Postgres (uma das opções):
    - DATABASE_URL (ou NEON_DSN / POSTGRES_URL / PG_URL)
    OU
    - PGHOST + PGDATABASE + PGUSER + PGPASSWORD (+ PGPORT opcional)

  Sync:
    - DB_SCHEMA (default visualizacao_resolvidos)
    - TABLE_NAME (default tickets_mesclados)
    - LOOKBACK_DAYS (default 30)
    - WINDOW_DAYS (default 7)
    - DRY_RUN (default false)

Uso:
  python sync_tickets_merged.py
"""

from __future__ import annotations

import os
import re
import time
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql


LOG = logging.getLogger("tickets_mesclados")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v


def parse_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "sim")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_movidesk_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fmt_movidesk_dt(dt: datetime) -> str:
    # Manual: UTC "YYYY-MM-DD HH:MM:SS"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class SyncCfg:
    movidesk_token: str
    base_url: str
    db_schema: str
    table_name: str
    lookback_days: int
    window_days: int
    dry_run: bool


# -----------------------------
# Postgres
# -----------------------------

def pg_connect() -> psycopg2.extensions.connection:
    """
    Aceita:
      - DATABASE_URL (prioridade)
      - NEON_DSN / POSTGRES_URL / PG_URL
      - ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD
    """
    dsn = (
        _env("DATABASE_URL")
        or _env("NEON_DSN")
        or _env("POSTGRES_URL")
        or _env("PG_URL")
    )
    if dsn:
        return psycopg2.connect(dsn)

    host = _env("PGHOST")
    db = _env("PGDATABASE")
    user = _env("PGUSER")
    pwd = _env("PGPASSWORD")
    port = _env("PGPORT", "5432")

    if not (host and db and user and pwd):
        raise RuntimeError(
            "Faltam variáveis de Postgres. Use DATABASE_URL (ou NEON_DSN/POSTGRES_URL/PG_URL) "
            "ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

    return psycopg2.connect(
        host=host,
        dbname=db,
        user=user,
        password=pwd,
        port=int(port) if port else 5432,
    )


def ensure_schema_and_table(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    """
    Cria schema/tabela (se não existir) e adiciona colunas novas (se necessário).
    Evita erros tipo: 'column ... does not exist'.
    """
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    ticket_id BIGINT PRIMARY KEY,
                    merged_tickets INTEGER,
                    merged_tickets_ids TEXT,
                    merged_ticket_ids_arr BIGINT[],
                    last_update TIMESTAMPTZ,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        def add_col(col: str, ddl: str) -> None:
            if col not in existing:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} " + ddl).format(
                        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(col)
                    )
                )

        add_col("merged_tickets", "INTEGER")
        add_col("merged_tickets_ids", "TEXT")
        add_col("merged_ticket_ids_arr", "BIGINT[]")
        add_col("last_update", "TIMESTAMPTZ")
        add_col("synced_at", "TIMESTAMPTZ NOT NULL DEFAULT now()")

    conn.commit()


def get_max_last_update(conn: psycopg2.extensions.connection, schema: str, table: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT max(last_update) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        dtv = row[0]
        if isinstance(dtv, datetime):
            if dtv.tzinfo is None:
                dtv = dtv.replace(tzinfo=timezone.utc)
            return dtv.astimezone(timezone.utc)
        return None


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
            synced_at = now()
        """
    ).format(sql.Identifier(schema), sql.Identifier(table))

    nowv = utc_now()
    values = [(a, b, c, d, e, nowv) for (a, b, c, d, e) in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, q.as_string(conn), values, page_size=1000)

    conn.commit()
    return len(rows)


# -----------------------------
# Movidesk API
# -----------------------------

class MovideskClient:
    def __init__(self, base_url: str, token: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> Tuple[int, Any]:
        url = f"{self.base_url}{path}"
        params = dict(params)
        params["token"] = self.token

        max_attempts = 6
        backoff = 2.0
        last_exc: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    LOG.warning("HTTP %s em %s (tentativa %s/%s).", r.status_code, path, attempt, max_attempts)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                    continue

                if r.status_code == 404:
                    # Em alguns cenários, o Movidesk devolve 404 quando não há resultados no período.
                    return 404, None

                r.raise_for_status()
                return r.status_code, r.json()
            except Exception as e:
                last_exc = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

        raise RuntimeError(f"Falha ao chamar Movidesk {path}: {last_exc}")

    @staticmethod
    def _parse_total_pages(page_number_field: Any) -> Optional[int]:
        if page_number_field is None:
            return None
        s = str(page_number_field)
        m = re.search(r"\b(\d+)\s*(?:of|de)\s*(\d+)\b", s, flags=re.IGNORECASE)
        if m:
            return int(m.group(2))
        return None

    def iter_merged_by_period(self, start_dt: datetime, end_dt: Optional[datetime]) -> Iterable[Dict[str, Any]]:
        page = 1
        total_pages: Optional[int] = None

        while True:
            params: Dict[str, Any] = {"startDate": fmt_movidesk_dt(start_dt), "page": page}
            if end_dt is not None:
                params["endDate"] = fmt_movidesk_dt(end_dt)

            status, data = self._get("/tickets/merged", params=params)

            if status == 404 or data is None:
                return

            if isinstance(data, dict):
                items = data.get("mergedTickets") or data.get("value") or []
                if total_pages is None:
                    total_pages = self._parse_total_pages(data.get("pageNumber"))
            elif isinstance(data, list):
                items = data
            else:
                items = []

            if not items:
                return

            for it in items:
                if isinstance(it, dict):
                    yield it

            if total_pages is not None and page >= total_pages:
                return

            page += 1
            time.sleep(0.15)


def normalize_record(rec: Dict[str, Any]) -> Optional[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]]:
    ticket_id = rec.get("ticketId") or rec.get("id")
    if ticket_id is None:
        return None

    try:
        ticket_id_i = int(str(ticket_id))
    except ValueError:
        return None

    merged_tickets = rec.get("mergedTickets")
    try:
        merged_tickets_i = int(str(merged_tickets)) if merged_tickets is not None else None
    except ValueError:
        merged_tickets_i = None

    merged_ids_text = rec.get("mergedTicketsIds")
    merged_ids_arr: Optional[List[int]] = None

    if merged_ids_text:
        parts = [p.strip() for p in str(merged_ids_text).split(";") if p.strip()]
        parsed: List[int] = []
        for p in parts:
            try:
                parsed.append(int(p))
            except ValueError:
                pass
        merged_ids_arr = parsed if parsed else None

    last_update = parse_movidesk_dt(rec.get("lastUpdate"))

    return (ticket_id_i, merged_tickets_i, str(merged_ids_text) if merged_ids_text is not None else None, merged_ids_arr, last_update)


# -----------------------------
# Main
# -----------------------------

def build_cfg_from_env(args: argparse.Namespace) -> SyncCfg:
    token = _env("MOVIDESK_TOKEN") or _env("MOVIDESK_API_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN).")

    base_url = _env("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")

    schema = _env("DB_SCHEMA", "visualizacao_resolvidos")
    table = _env("TABLE_NAME", "tickets_mesclados")

    lookback_days = int(_env("LOOKBACK_DAYS", str(args.lookback_days)))
    window_days = int(_env("WINDOW_DAYS", str(args.window_days)))

    dry_run = parse_bool(_env("DRY_RUN"), default=args.dry_run)

    return SyncCfg(
        movidesk_token=token,
        base_url=base_url,
        db_schema=schema,
        table_name=table,
        lookback_days=lookback_days,
        window_days=window_days,
        dry_run=dry_run,
    )


def daterange_windows(start: datetime, end: datetime, window_days: int) -> Iterable[Tuple[datetime, datetime]]:
    if window_days <= 0:
        yield (start, end)
        return

    cur = start
    delta = timedelta(days=window_days)
    while cur < end:
        nxt = min(cur + delta, end)
        yield (cur, nxt)
        cur = nxt


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--since", type=str, default=None, help="Override início (UTC). Ex: 2025-12-01 00:00:00")
    ap.add_argument("--until", type=str, default=None, help="Override fim (UTC). Ex: 2025-12-24 23:59:59")
    args = ap.parse_args()

    cfg = build_cfg_from_env(args)

    now = utc_now()
    until_dt = parse_movidesk_dt(args.until) if args.until else now
    if until_dt is None:
        until_dt = now

    since_dt = parse_movidesk_dt(args.since) if args.since else None

    conn: Optional[psycopg2.extensions.connection] = None
    if not cfg.dry_run:
        conn = pg_connect()
        conn.autocommit = False
        ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

        if since_dt is None:
            max_dt = get_max_last_update(conn, cfg.db_schema, cfg.table_name)
            if max_dt:
                # folga para capturar ajustes de última hora
                since_dt = max_dt - timedelta(days=2)

    if since_dt is None:
        since_dt = (until_dt - timedelta(days=cfg.lookback_days))

    LOG.info(
        "sync iniciando | schema=%s tabela=%s | intervalo=%s..%s | lookback_days=%s | window_days=%s | dry_run=%s",
        cfg.db_schema, cfg.table_name,
        since_dt.date().isoformat(), until_dt.date().isoformat(),
        cfg.lookback_days, cfg.window_days, cfg.dry_run,
    )

    client = MovideskClient(cfg.base_url, cfg.movidesk_token)

    total_upserted = 0
    total_fetched = 0

    for w_start, w_end in daterange_windows(since_dt, until_dt, cfg.window_days):
        LOG.info("buscando /tickets/merged startDate=%s endDate=%s", w_start.date().isoformat(), w_end.date().isoformat())

        batch: List[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]] = []

        for rec in client.iter_merged_by_period(w_start, w_end):
            total_fetched += 1
            norm = normalize_record(rec)
            if not norm:
                continue
            batch.append(norm)

            if len(batch) >= 2000:
                if cfg.dry_run:
                    LOG.info("dry-run: batch=%s (exemplo=%s)", len(batch), batch[0])
                    batch.clear()
                    continue

                assert conn is not None
                total_upserted += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)
                batch.clear()

        if batch:
            if cfg.dry_run:
                LOG.info("dry-run: batch=%s (exemplo=%s)", len(batch), batch[0])
            else:
                assert conn is not None
                total_upserted += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)

    if cfg.dry_run:
        LOG.info("sync concluída (dry-run). fetched=%s", total_fetched)
        return

    assert conn is not None
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(cfg.db_schema), sql.Identifier(cfg.table_name)
        ))
        total_db = cur.fetchone()[0]

    conn.commit()
    conn.close()

    LOG.info("sync concluída. fetched=%s upserted=%s total_na_tabela=%s", total_fetched, total_upserted, total_db)


if __name__ == "__main__":
    main()
