#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza merges de tickets do Movidesk usando o endpoint /tickets/merged.

Baseado no manual "API do Movidesk - Tickets/Merged" (parâmetros: token obrigatório;
id opcional; startDate/endDate opcionais; page opcional).

Principais ajustes vs implementações que quebram com 404/400:
- startDate/endDate são enviados como DATA (YYYY-MM-DD), sem hora (o manual mostra exemplos assim).
- Não usa campos inexistentes no /tickets ($select=mergedTicketsIds etc.) para evitar 400.
- Garante que a tabela tenha a coluna last_update (e faz rollback em erros para não “matar” a transação).

Variáveis de ambiente:
- MOVIDESK_TOKEN (obrigatório)
- MOVIDESK_BASE_URL (opcional, default: https://api.movidesk.com/public/v1)

Conexão PostgreSQL:
- DATABASE_URL (ex: postgres://user:pass@host:5432/db)  OU
- PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

Config:
- PG_SCHEMA (default: visualizacao_resolvidos)
- MERGED_TABLE (default: tickets_mesclados)
- STATE_TABLE (default: sync_state)
- LOOKBACK_DAYS (default: 7)  -> usado quando não existe estado salvo
- CHUNK_DAYS (default: 7)     -> janela em dias por chamada
- HTTP_TIMEOUT (default: 60)
- HTTP_RETRIES (default: 6)

Saída:
- Faz UPSERT em {schema}.{table} com (ticket_id, ticket_merged_id) como chave.
- Atualiza estado em {schema}.{state_table} na chave 'tickets_merged_last_run_utc'.
"""

from __future__ import annotations

import os
import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# ----------------------------
# Util
# ----------------------------

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_date_yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_iso_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        return None

def chunk_date_range(start: date, end: date, chunk_days: int) -> Iterable[Tuple[date, date]]:
    """Gera janelas inclusivas [start, end] em blocos de chunk_days."""
    if chunk_days <= 0:
        chunk_days = 7
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        yield cur, nxt
        cur = nxt + timedelta(days=1)

def log(msg: str) -> None:
    print(msg, flush=True)


# ----------------------------
# Movidesk API
# ----------------------------

@dataclass(frozen=True)
class MovideskClient:
    token: str
    base_url: str = "https://api.movidesk.com/public/v1"
    timeout: int = 60
    retries: int = 6

    def _request(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        params = dict(params)
        params["token"] = self.token

        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)

                if resp.status_code in (429, 500, 502, 503, 504):
                    last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
                    sleep_s = min(60, (2 ** (attempt - 1)) + (attempt * 0.25))
                    log(f"[WARN] Movidesk {path}: HTTP {resp.status_code}, retry {attempt}/{self.retries} em {sleep_s:.1f}s")
                    time.sleep(sleep_s)
                    continue

                if resp.status_code >= 400:
                    body_preview = resp.text[:1000]
                    raise requests.HTTPError(
                        f"{resp.status_code} Client/Server Error for url: {resp.url}\n{body_preview}",
                        response=resp,
                    )

                if resp.text.strip() == "":
                    return None
                return resp.json()

            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = e
                sleep_s = min(60, (2 ** (attempt - 1)) + (attempt * 0.25))
                log(f"[WARN] Movidesk {path}: erro de rede/timeout ({e}), retry {attempt}/{self.retries} em {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

        raise RuntimeError(f"Falha após {self.retries} tentativas em {path}: {last_err}")

    def list_tickets_merged(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Busca merges por período (manual exemplifica startDate/endDate no formato YYYY-MM-DD).
        """
        all_rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = {
                "startDate": to_date_yyyy_mm_dd(start_date),
                "endDate": to_date_yyyy_mm_dd(end_date),
                "page": page,
            }
            log(f"tickets_mesclados: GET /tickets/merged startDate={params['startDate']} endDate={params['endDate']} page={page}")
            data = self._request("/tickets/merged", params=params)

            if data is None:
                break
            if isinstance(data, dict):
                raise RuntimeError(f"Resposta inesperada (dict) em /tickets/merged: {json.dumps(data)[:1200]}")
            if not isinstance(data, list):
                raise RuntimeError(f"Resposta inesperada (tipo {type(data)}) em /tickets/merged")

            if len(data) == 0:
                break

            all_rows.extend(data)
            page += 1

            # Heurística: se retornar poucos, provavelmente acabou.
            if len(data) < 100:
                break

        return all_rows


# ----------------------------
# Postgres
# ----------------------------

def get_pg_conn():
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pw = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not (host and db and user and pw):
        raise RuntimeError("Faltando variáveis de conexão Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)

def ensure_schema_and_tables(conn, schema: str, table: str, state_table: str) -> None:
    ddl = f"""
    create schema if not exists {schema};

    create table if not exists {schema}.{state_table} (
        name text primary key,
        value timestamptz
    );

    create table if not exists {schema}.{table} (
        ticket_id bigint not null,
        ticket_merged_id bigint not null,
        last_update timestamptz,
        range_start date,
        range_end date,
        raw jsonb,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        primary key (ticket_id, ticket_merged_id)
    );

    create index if not exists idx_{table}_last_update on {schema}.{table}(last_update);
    """
    alter = f"""
    alter table {schema}.{table} add column if not exists last_update timestamptz;
    alter table {schema}.{table} add column if not exists range_start date;
    alter table {schema}.{table} add column if not exists range_end date;
    alter table {schema}.{table} add column if not exists raw jsonb;
    alter table {schema}.{table} add column if not exists created_at timestamptz not null default now();
    alter table {schema}.{table} add column if not exists updated_at timestamptz not null default now();
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(alter)
    conn.commit()

def get_state(conn, schema: str, state_table: str, key: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(f"select value from {schema}.{state_table} where name = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def set_state(conn, schema: str, state_table: str, key: str, value: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {schema}.{state_table}(name, value)
            values (%s, %s)
            on conflict (name) do update set value = excluded.value
            """,
            (key, value),
        )
    conn.commit()

def upsert_merges(conn, schema: str, table: str, merges: List[Dict[str, Any]], range_start: date, range_end: date) -> int:
    if not merges:
        return 0

    rows = []
    for m in merges:
        ticket_id = m.get("ticketId")
        merged_id = m.get("ticketMergedId")
        if ticket_id is None or merged_id is None:
            continue

        last_update = parse_iso_dt(m.get("lastUpdate", ""))
        raw = json.dumps(m, ensure_ascii=False)
        rows.append((int(ticket_id), int(merged_id), last_update, range_start, range_end, raw))

    if not rows:
        return 0

    sql = f"""
    insert into {schema}.{table}
        (ticket_id, ticket_merged_id, last_update, range_start, range_end, raw, updated_at)
    values %s
    on conflict (ticket_id, ticket_merged_id) do update set
        last_update = excluded.last_update,
        range_start = excluded.range_start,
        range_end = excluded.range_end,
        raw = excluded.raw,
        updated_at = now()
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)

def safe_count(conn, schema: str, table: str) -> int:
    try:
        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {schema}.{table}")
            return int(cur.fetchone()[0])
    except Exception:
        conn.rollback()
        raise


# ----------------------------
# Main
# ----------------------------

STATE_KEY = "tickets_merged_last_run_utc"

def main() -> None:
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVI_TOKEN") or os.getenv("TOKEN_MOVIDESK")
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN não definido.")

    base_url = os.getenv("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
    timeout = env_int("HTTP_TIMEOUT", 60)
    retries = env_int("HTTP_RETRIES", 6)

    schema = os.getenv("PG_SCHEMA", "visualizacao_resolvidos")
    table = os.getenv("MERGED_TABLE", "tickets_mesclados")
    state_table = os.getenv("STATE_TABLE", "sync_state")

    lookback_days = env_int("LOOKBACK_DAYS", 7)
    chunk_days = env_int("CHUNK_DAYS", 7)

    client = MovideskClient(token=token, base_url=base_url, timeout=timeout, retries=retries)

    conn = None
    try:
        conn = get_pg_conn()
        conn.autocommit = False
        ensure_schema_and_tables(conn, schema=schema, table=table, state_table=state_table)

        last_run = get_state(conn, schema=schema, state_table=state_table, key=STATE_KEY)
        now = utcnow()

        if last_run is None:
            start_dt = now - timedelta(days=lookback_days)
            log(f"tickets_mesclados: sem estado anterior; usando LOOKBACK_DAYS={lookback_days} -> {start_dt.isoformat()}")
        else:
            start_dt = last_run
            log(f"tickets_mesclados: último estado={last_run.isoformat()}")

        # Buffer de 1 dia (API trabalha por data; melhor reprocessar 1 dia e evitar “buraco”)
        start_date = (start_dt.date() - timedelta(days=1))
        end_date = now.date()

        total_upserted = 0
        total_fetched = 0

        for a, b in chunk_date_range(start_date, end_date, chunk_days=chunk_days):
            try:
                merges = client.list_tickets_merged(a, b)
            except requests.HTTPError as e:
                msg = str(e)
                if "404" in msg:
                    log(
                        "[WARN] /tickets/merged retornou 404 neste ambiente. "
                        "Confirme se o endpoint existe para sua conta/ambiente e se as datas estão no formato YYYY-MM-DD. "
                        f"Detalhe: {msg.splitlines()[0]}"
                    )
                    break
                raise

            total_fetched += len(merges)
            if merges:
                up = upsert_merges(conn, schema=schema, table=table, merges=merges, range_start=a, range_end=b)
                total_upserted += up
                log(f"tickets_mesclados: janela {to_date_yyyy_mm_dd(a)}..{to_date_yyyy_mm_dd(b)} -> fetched={len(merges)} upserted={up}")
            else:
                log(f"tickets_mesclados: janela {to_date_yyyy_mm_dd(a)}..{to_date_yyyy_mm_dd(b)} -> nenhum merge")

        set_state(conn, schema=schema, state_table=state_table, key=STATE_KEY, value=now)

        count_total = safe_count(conn, schema=schema, table=table)
        log(f"tickets_mesclados: sincronização concluída. fetched={total_fetched} upserted={total_upserted} total_na_tabela={count_total}.")

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        log("[ERRO] Falha na sincronização de tickets_mesclados.")
        log(traceback.format_exc())
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
