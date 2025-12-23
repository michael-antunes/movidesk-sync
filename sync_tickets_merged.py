#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sync de Tickets Mesclados (Movidesk) -> Postgres

Endpoint base:
  GET https://api.movidesk.com/public/v1/tickets/merged

Modos:
  - ticketId=<id>  (retorna um objeto)
  - startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&pageNumber=N (retorna paginação com mergedTickets)

Este script:
  - varre um período (lookback) em janelas (window_days)
  - pagina pageNumber=1..N
  - normaliza mergedTicketsIds (separado por ';') em linhas:
        (ticket_id, merged_ticket_id)
  - grava em Postgres

Variáveis de ambiente:
  # Movidesk
  MOVIDESK_API_TOKEN   (obrigatório)
  MOVIDESK_BASE_URL    (opcional, default: https://api.movidesk.com/public/v1)

  # Postgres
  DATABASE_URL         (recomendado)  ex: postgresql://user:pass@host:5432/dbname
    OU
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

  # Tabela destino
  DB_SCHEMA            (default: visualizacao_resolvidos)
  TABLE_NAME           (default: tickets_mesclados)

  # Período
  LOOKBACK_DAYS        (default: 30)  usado quando tabela ainda não tem last_update
  WINDOW_DAYS          (default: 7)   tamanho da janela para chamadas no /tickets/merged
  START_DATE           (opcional)     força início (YYYY-MM-DD)
  END_DATE             (opcional)     força fim (YYYY-MM-DD)

  # Modo unitário
  TICKET_ID            (opcional)     se setado, consulta apenas ticketId e grava
"""

from __future__ import annotations

import os
import sys
import time
import random
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# ---------------------------
# Utilidades
# ---------------------------

def utc_today() -> date:
    return datetime.now(timezone.utc).date()

def parse_date_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()

def parse_last_update(s: Optional[str]) -> Optional[datetime]:
    """Manual do Tickets_Merged mostra lastUpdate como 'YYYY-MM-DD HH:mm:ss'."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v)

def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not str(v).strip():
        raise RuntimeError(f"Variável obrigatória ausente: {name}")
    return v.strip()


# ---------------------------
# Config
# ---------------------------

@dataclass
class Config:
    token: str
    base_url: str
    db_schema: str
    table_name: str
    lookback_days: int
    window_days: int
    start_date: Optional[date]
    end_date: Optional[date]
    ticket_id: Optional[int]

@dataclass
class DbCapabilities:
    has_unique_pair_index: bool

def load_config() -> Config:
    token = required_env("MOVIDESK_API_TOKEN")
    base_url = env_str("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1").rstrip("/")

    db_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    table_name = env_str("TABLE_NAME", "tickets_mesclados")

    lookback_days = env_int("LOOKBACK_DAYS", 30)
    window_days = max(1, env_int("WINDOW_DAYS", 7))

    start_date = parse_date_yyyy_mm_dd(os.getenv("START_DATE")) if os.getenv("START_DATE") else None
    end_date = parse_date_yyyy_mm_dd(os.getenv("END_DATE")) if os.getenv("END_DATE") else None
    ticket_id = int(os.getenv("TICKET_ID")) if os.getenv("TICKET_ID") else None

    if start_date and end_date and end_date < start_date:
        raise RuntimeError("END_DATE não pode ser menor que START_DATE")

    return Config(
        token=token,
        base_url=base_url,
        db_schema=db_schema,
        table_name=table_name,
        lookback_days=lookback_days,
        window_days=window_days,
        start_date=start_date,
        end_date=end_date,
        ticket_id=ticket_id,
    )


# ---------------------------
# Postgres
# ---------------------------

def pg_connect():
    """Prioriza DATABASE_URL. Alternativa: PGHOST/PGDATABASE/PGUSER/PGPASSWORD."""
    dsn = os.getenv("DATABASE_URL", "").strip()
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pw = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not (host and db and user and pw):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)

def ensure_schema_and_table(conn, schema: str, table: str) -> DbCapabilities:
    """Cria schema/tabela e garante colunas necessárias.

    Também tenta criar um índice único em (ticket_id, merged_ticket_id). Se não conseguir
    (ex.: duplicados), cai para estratégia delete+insert.
    """
    ddl = f'''
    create schema if not exists "{schema}";

    create table if not exists "{schema}"."{table}" (
        ticket_id bigint not null,
        merged_ticket_id bigint not null,
        merged_ticket_ids_raw text null,
        merged_tickets_count integer null,
        last_update timestamp null,
        source text not null default 'tickets/merged',
        synced_at timestamptz not null default now()
    );
    '''
    idx_name = f"{table}_uidx_ticket_merged_pair"
    idx_sql = f'''create unique index if not exists "{idx_name}" on "{schema}"."{table}" (ticket_id, merged_ticket_id);'''

    with conn.cursor() as cur:
        cur.execute(ddl)

        # garante colunas mesmo se a tabela já existia
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists ticket_id bigint;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists merged_ticket_id bigint;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists merged_ticket_ids_raw text;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists merged_tickets_count integer;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists last_update timestamp;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists source text;')
        cur.execute(f'alter table "{schema}"."{table}" add column if not exists synced_at timestamptz;')

        cur.execute(f'''alter table "{schema}"."{table}" alter column source set default 'tickets/merged';''')
        cur.execute(f'''alter table "{schema}"."{table}" alter column synced_at set default now();''')

        has_unique = True
        try:
            cur.execute(idx_sql)
        except Exception as e:
            has_unique = False
            print(
                f"[WARN] não consegui criar índice único ({idx_name}) em ({schema}.{table}). "
                f"Vou usar delete+insert (sem ON CONFLICT). Motivo: {e}"
            )

    conn.commit()
    return DbCapabilities(has_unique_pair_index=has_unique)

def get_max_last_update(conn, schema: str, table: str) -> Optional[datetime]:
    sql = f'''select max(last_update) from "{schema}"."{table}"'''
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row or not row[0]:
        return None
    dt = row[0]
    if isinstance(dt, datetime) and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _delete_existing_pairs(conn, schema: str, table: str, pairs: List[Tuple[int, int]]) -> None:
    if not pairs:
        return
    with conn.cursor() as cur:
        for i in range(0, len(pairs), 2000):
            ch = pairs[i:i+2000]
            placeholders = ",".join(["(%s,%s)"] * len(ch))
            flat: List[int] = []
            for a, b in ch:
                flat.extend([a, b])
            cur.execute(
                f'''delete from "{schema}"."{table}" where (ticket_id, merged_ticket_id) in ({placeholders})''',
                flat
            )

def upsert_rows(conn, schema: str, table: str, rows: List[Dict[str, Any]], caps: DbCapabilities) -> int:
    if not rows:
        return 0

    now_utc = datetime.now(timezone.utc)
    values = []
    pairs = []
    for r in rows:
        if r.get("ticket_id") is None or r.get("merged_ticket_id") is None:
            continue

        last_upd = r.get("last_update")
        if isinstance(last_upd, datetime):
            last_upd = last_upd.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            last_upd = None

        tid = int(r["ticket_id"])
        mid = int(r["merged_ticket_id"])
        pairs.append((tid, mid))

        values.append((
            tid,
            mid,
            r.get("merged_ticket_ids_raw"),
            r.get("merged_tickets_count"),
            last_upd,
            r.get("source", "tickets/merged"),
            now_utc,
        ))

    if not values:
        return 0

    if caps.has_unique_pair_index:
        sql = f'''
            insert into "{schema}"."{table}"
                (ticket_id, merged_ticket_id, merged_ticket_ids_raw, merged_tickets_count, last_update, source, synced_at)
            values %s
            on conflict (ticket_id, merged_ticket_id) do update set
                merged_ticket_ids_raw = excluded.merged_ticket_ids_raw,
                merged_tickets_count  = excluded.merged_tickets_count,
                last_update           = excluded.last_update,
                source                = excluded.source,
                synced_at             = excluded.synced_at
        '''
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        conn.commit()
        return len(values)

    # fallback: delete + insert
    _delete_existing_pairs(conn, schema, table, pairs)
    sql2 = f'''
        insert into "{schema}"."{table}"
            (ticket_id, merged_ticket_id, merged_ticket_ids_raw, merged_tickets_count, last_update, source, synced_at)
        values %s
    '''
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql2, values, page_size=1000)
    conn.commit()
    return len(values)


# ---------------------------
# Movidesk client
# ---------------------------

class MovideskClient:
    def __init__(self, base_url: str, token: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> requests.Response:
        url = f"{self.base_url}{path}"
        params = dict(params)
        params["token"] = self.token

        max_tries = 5
        for attempt in range(1, max_tries + 1):
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(60, (2 ** attempt) + random.random())
                print(f"[WARN] HTTP {resp.status_code} em {path} (tentativa {attempt}/{max_tries}). aguardando {wait:.1f}s...")
                time.sleep(wait)
                continue
            return resp
        return resp

    def get_merged_by_ticket_id(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        resp = self._get("/tickets/merged", {"ticketId": ticket_id})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("ticketId"):
            return data
        return None

    def iter_merged_in_range(self, start: date, end: date) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            params = {
                "startDate": start.strftime("%Y-%m-%d"),
                "endDate": end.strftime("%Y-%m-%d"),
                "pageNumber": page,
            }
            resp = self._get("/tickets/merged", params)

            # 404 pode significar "sem resultados" para o filtro
            if resp.status_code == 404:
                return

            resp.raise_for_status()
            data = resp.json()

            items: List[Dict[str, Any]] = []
            if isinstance(data, dict) and isinstance(data.get("mergedTickets"), list):
                items = data.get("mergedTickets") or []
            elif isinstance(data, list):
                items = data
            else:
                print(f"[WARN] resposta inesperada em /tickets/merged: {type(data)}")
                return

            if not items:
                return

            for it in items:
                if isinstance(it, dict):
                    yield it

            page += 1


# ---------------------------
# Normalização
# ---------------------------

def normalize_items(items: Iterable[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        ticket_id = it.get("ticketId") or it.get("id")
        if ticket_id is None:
            continue

        merged_ids_raw = it.get("mergedTicketsIds") or ""
        merged_count = it.get("mergedTickets")
        last_update = parse_last_update(it.get("lastUpdate"))

        merged_ids: List[int] = []
        if isinstance(merged_ids_raw, str) and merged_ids_raw.strip():
            for part in merged_ids_raw.split(";"):
                part = part.strip()
                if part and part.isdigit():
                    merged_ids.append(int(part))

        # remove duplicados mantendo ordem
        seen = set()
        merged_ids_unique: List[int] = []
        for mid in merged_ids:
            if mid not in seen:
                merged_ids_unique.append(mid)
                seen.add(mid)

        if not merged_ids_unique:
            if merged_count and str(merged_count).isdigit() and int(merged_count) > 0:
                print(f"[WARN] ticket {ticket_id} veio com mergedTickets={merged_count} mas sem mergedTicketsIds. ignorando.")
            continue

        for mid in merged_ids_unique:
            out.append({
                "ticket_id": int(ticket_id),
                "merged_ticket_id": int(mid),
                "merged_ticket_ids_raw": merged_ids_raw if isinstance(merged_ids_raw, str) else None,
                "merged_tickets_count": int(merged_count) if merged_count is not None and str(merged_count).isdigit() else None,
                "last_update": last_update,
                "source": source,
            })
    return out


# ---------------------------
# Main
# ---------------------------

def date_windows(start: date, end: date, window_days: int) -> List[Tuple[date, date]]:
    windows: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        w_end = min(end, cur + timedelta(days=window_days - 1))
        windows.append((cur, w_end))
        cur = w_end + timedelta(days=1)
    return windows

def main() -> None:
    cfg = load_config()

    conn = pg_connect()
    conn.autocommit = False
    caps = ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

    client = MovideskClient(cfg.base_url, cfg.token)

    # Modo unitário
    if cfg.ticket_id is not None:
        print(f"tickets_mesclados: consultando ticketId={cfg.ticket_id} em /tickets/merged")
        data = client.get_merged_by_ticket_id(cfg.ticket_id)
        if not data:
            print("tickets_mesclados: nenhum merge encontrado para esse ticket.")
            return
        rows = normalize_items([data], source="tickets/merged(ticketId)")
        total = upsert_rows(conn, cfg.db_schema, cfg.table_name, rows, caps)
        print(f"tickets_mesclados: gravação concluída | linhas={total}")
        return

    # Período
    today = utc_today()
    end = cfg.end_date or today
    start = cfg.start_date

    if start is None:
        max_dt = get_max_last_update(conn, cfg.db_schema, cfg.table_name)
        if max_dt:
            start = max_dt.astimezone(timezone.utc).date() - timedelta(days=1)
            print(f"tickets_mesclados: incremental | max(last_update)={max_dt.isoformat()} -> start={start.isoformat()}")
        else:
            start = end - timedelta(days=cfg.lookback_days)
            print(f"tickets_mesclados: primeira carga | lookback_days={cfg.lookback_days} -> start={start.isoformat()}")

    if start > end:
        print(f"[WARN] intervalo vazio: start={start} > end={end}. nada a fazer.")
        return

    windows = date_windows(start, end, cfg.window_days)
    print(
        f"tickets_mesclados: sync iniciando | schema={cfg.db_schema} tabela={cfg.table_name} | "
        f"janela={start.isoformat()}..{end.isoformat()} (window_days={cfg.window_days}) | janelas={len(windows)}"
    )

    total_rows = 0
    for w_start, w_end in windows:
        print(f"tickets_mesclados: buscando /tickets/merged startDate={w_start} endDate={w_end}")
        items = list(client.iter_merged_in_range(w_start, w_end))
        rows = normalize_items(items, source="tickets/merged(range)")
        batch_total = upsert_rows(conn, cfg.db_schema, cfg.table_name, rows, caps)
        total_rows += batch_total
        print(f"tickets_mesclados: janela {w_start}..{w_end} | itens_api={len(items)} | linhas_gravadas={batch_total}")

    with conn.cursor() as cur:
        cur.execute(f'''select count(*) from "{cfg.db_schema}"."{cfg.table_name}"''')
        total_in_table = cur.fetchone()[0]
    conn.commit()
    conn.close()

    print(
        f"tickets_mesclados: sincronização concluída | "
        f"linhas_gravadas_total={total_rows} | total_na_tabela={total_in_table}"
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise
