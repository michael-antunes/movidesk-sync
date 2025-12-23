#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sync de Tickets Mesclados (Movidesk) -> Postgres

Baseado no manual "API do Movidesk - Tickets_Merged":
- GET /tickets/merged?token=...&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&page=N
- GET /tickets/merged?token=...&id=ID

Estratégia:
1) Tenta sync principal via /tickets/merged por janela de datas (YYYY-MM-DD).
2) Se receber 404 no endpoint principal, usa fallback:
   - lista tickets atualizados recentemente via /tickets (OData lastUpdate)
   - consulta /tickets/merged?id=... para cada ticket (404 => sem merge)

Saída em Postgres:
Schema: visualizacao_resolvidos (default)
Tabela: tickets_mesclados
PK: (ticket_id, merged_ticket_id)
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests


# -----------------------------
# Config
# -----------------------------

DEFAULT_API_BASE = "https://api.movidesk.com/public/v1"
DEFAULT_SCHEMA = "visualizacao_resolvidos"
DEFAULT_TABLE = "tickets_mesclados"
DEFAULT_STATE_TABLE = "sync_state"

SYNC_KEY_LAST_END = "tickets_merged_last_end_utc"
SYNC_KEY_LAST_OK = "tickets_merged_last_ok_utc"

# janela de datas do endpoint merged (em dias)
DEFAULT_RANGE_DAYS = 7

# overlap pra não perder merges (como a API por data é por dia)
DEFAULT_OVERLAP_DAYS = 2

# fallback: quantos tickets no máximo checar por execução
DEFAULT_FALLBACK_MAX_IDS = 800

# requests
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 2.0


# -----------------------------
# Helpers de tempo
# -----------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_utc(s: str) -> datetime:
    # aceita "2025-12-23T13:02:44.123456+00:00" ou "...Z"
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def to_odata_utc(dt: datetime) -> str:
    # Ex: 2024-01-01T00:00:00Z
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_date_str(d: date) -> str:
    # manual do Tickets_Merged usa YYYY-MM-DD
    return d.strftime("%Y-%m-%d")


def parse_last_update(value: Any) -> Optional[datetime]:
    """
    No manual do Tickets_Merged, lastUpdate aparece como: "2023-10-02 18:20:40"
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        # não esperado, mas trata como epoch
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    s = str(value).strip()
    if not s:
        return None

    # tenta ISO primeiro
    try:
        if s.endswith("Z"):
            return parse_iso_utc(s)
        if "T" in s:
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # tenta "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# -----------------------------
# Movidesk Client
# -----------------------------

class MovideskClient:
    def __init__(self, token: str, base_url: str = DEFAULT_API_BASE, timeout: int = DEFAULT_TIMEOUT):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _request(self, path: str, params: Dict[str, Any], retries: int = DEFAULT_RETRIES) -> requests.Response:
        url = f"{self.base_url}{path}"
        params = dict(params)
        params["token"] = self.token

        attempt = 0
        backoff = DEFAULT_BACKOFF_SECONDS

        while True:
            attempt += 1
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt >= retries:
                    raise
                print(f"[WARN] erro de rede ao chamar {path} (tentativa {attempt}/{retries}): {e}")
                time.sleep(backoff)
                backoff *= 2
                continue

            # rate limit / instabilidade
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= retries:
                    return resp
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_s = int(retry_after)
                else:
                    sleep_s = backoff
                    backoff *= 2
                print(f"[WARN] HTTP {resp.status_code} em {path}, retry em {sleep_s}s (tentativa {attempt}/{retries})")
                time.sleep(sleep_s)
                continue

            return resp

    def get_merged_by_date(self, start_d: date, end_d: date, page: int) -> requests.Response:
        return self._request(
            "/tickets/merged",
            params={
                "startDate": to_date_str(start_d),
                "endDate": to_date_str(end_d),
                "page": page,
            },
        )

    def get_merged_by_id(self, ticket_id: int) -> requests.Response:
        return self._request(
            "/tickets/merged",
            params={"id": str(ticket_id)},
        )

    def list_ticket_ids_updated_since(self, since_utc: datetime, top: int = 1000, skip: int = 0) -> requests.Response:
        # /tickets suporta OData; evita campos inexistentes (que davam 400 no seu log)
        # Exemplo do manual: createdDate ge 2024-01-01T00:00:00Z
        since_s = to_odata_utc(since_utc)
        return self._request(
            "/tickets",
            params={
                "$select": "id,lastUpdate",
                "$filter": f"lastUpdate ge {since_s}",
                "$orderby": "lastUpdate asc",
                "$top": str(top),
                "$skip": str(skip),
            },
        )


# -----------------------------
# Parsing de resposta
# -----------------------------

def parse_page_number(page_number: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    pageNumber vem como "1 of 333" (manual).
    """
    if not page_number:
        return None, None
    s = str(page_number)
    m = re.search(r"(\d+)\s*of\s*(\d+)", s, flags=re.IGNORECASE)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def parse_merged_ids(item: Dict[str, Any]) -> List[int]:
    """
    mergedTicketsIds pode vir como:
      - string "123;456;789"
      - lista [123, 456]
      - ou vazio
    """
    v = item.get("mergedTicketsIds")
    if v is None:
        v = item.get("mergedTicketsId")  # por segurança (algumas integrações usam singular)
    if v is None:
        return []

    if isinstance(v, list):
        out = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return sorted(set(out))

    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(";") if p.strip()]
    out = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return sorted(set(out))


@dataclass(frozen=True)
class EdgeRow:
    ticket_id: int
    merged_ticket_id: int
    last_update: Optional[datetime]
    raw: Dict[str, Any]


def edges_from_merged_item(item: Dict[str, Any]) -> List[EdgeRow]:
    tid = item.get("ticketId")
    if tid is None:
        # algumas respostas podem usar "id"
        tid = item.get("id")
    if tid is None:
        return []

    try:
        ticket_id = int(tid)
    except Exception:
        return []

    last_update = parse_last_update(item.get("lastUpdate"))
    merged_ids = parse_merged_ids(item)

    edges: List[EdgeRow] = []
    for mid in merged_ids:
        if mid == ticket_id:
            continue
        edges.append(EdgeRow(ticket_id=ticket_id, merged_ticket_id=int(mid), last_update=last_update, raw=item))
    return edges


# -----------------------------
# Postgres
# -----------------------------

def pg_connect():
    """
    Aceita:
      - DATABASE_URL (ex: postgres://user:pass@host:5432/db)
    ou:
      - PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")

    if not all([host, db, user, pwd]):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(host=host, port=int(port), dbname=db, user=user, password=pwd)


def ensure_schema_and_tables(conn, schema: str, table: str, state_table: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Tabela principal (edges)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                ticket_id BIGINT NOT NULL,
                merged_ticket_id BIGINT NOT NULL,
                last_update TIMESTAMPTZ NULL,
                raw JSONB NOT NULL,
                inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (ticket_id, merged_ticket_id)
            );
        """)

        # Garante colunas (pra evitar seu erro "column last_update does not exist")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS last_update TIMESTAMPTZ NULL;")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS raw JSONB NOT NULL DEFAULT '{{}}'::jsonb;")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT now();")
        cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();")

        # Índices úteis
        cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_ticket_id ON {schema}.{table}(ticket_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_merged_ticket_id ON {schema}.{table}(merged_ticket_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_last_update ON {schema}.{table}(last_update);")

        # Tabela de estado
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{state_table} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

    conn.commit()


def state_get(conn, schema: str, state_table: str, key: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT value FROM {schema}.{state_table} WHERE key=%s;", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def state_set(conn, schema: str, state_table: str, key: str, value: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.{state_table}(key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key)
            DO UPDATE SET value=EXCLUDED.value, updated_at=now();
            """,
            (key, value),
        )
    conn.commit()


def upsert_edges(conn, schema: str, table: str, edges: Sequence[EdgeRow]) -> int:
    if not edges:
        return 0

    now = utcnow()
    rows = [
        (
            e.ticket_id,
            e.merged_ticket_id,
            e.last_update,
            psycopg2.extras.Json(e.raw),
            now,
            now,
        )
        for e in edges
    ]

    sql = f"""
        INSERT INTO {schema}.{table}
            (ticket_id, merged_ticket_id, last_update, raw, inserted_at, updated_at)
        VALUES %s
        ON CONFLICT (ticket_id, merged_ticket_id)
        DO UPDATE SET
            last_update = EXCLUDED.last_update,
            raw = EXCLUDED.raw,
            updated_at = now();
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)

    conn.commit()
    return len(edges)


# -----------------------------
# Sync principal (/tickets/merged por data)
# -----------------------------

def fetch_all_merged_in_range(client: MovideskClient, start_d: date, end_d: date) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    page = 1
    total_pages: Optional[int] = None

    while True:
        resp = client.get_merged_by_date(start_d, end_d, page)

        if resp.status_code == 404:
            # Para date-range, 404 normalmente indica endpoint/rota indisponível nesse tenant
            # (ou URL base errada). Tratamos como falha "estrutural" pra acionar fallback.
            snippet = resp.text[:300].replace("\n", " ")
            raise RuntimeError(f"/tickets/merged retornou 404 na busca por data (page={page}). Trecho: {snippet}")

        if resp.status_code >= 400:
            snippet = resp.text[:500].replace("\n", " ")
            raise RuntimeError(f"/tickets/merged HTTP {resp.status_code} (page={page}). Trecho: {snippet}")

        data = resp.json()
        # esperado: {"tickets":[...], "pageNumber":"1 of 333", ...}
        tickets = data.get("tickets")
        if tickets is None:
            # fallback: às vezes pode vir como lista
            if isinstance(data, list):
                tickets = data
            else:
                tickets = []

        if not tickets:
            break

        items.extend(tickets)

        _, tp = parse_page_number(data.get("pageNumber"))
        if tp:
            total_pages = tp

        if total_pages is not None and page >= total_pages:
            break

        page += 1

    return items


# -----------------------------
# Fallback seguro (sem 400)
# -----------------------------

def fallback_by_recent_updates(
    client: MovideskClient,
    since_utc: datetime,
    max_ids: int,
) -> List[Dict[str, Any]]:
    """
    1) Busca IDs de tickets atualizados desde since_utc via /tickets (OData lastUpdate).
    2) Para cada ID, consulta /tickets/merged?id=...
    Retorna lista de itens no formato do /tickets/merged por id (cada item é 1 ticketId).
    """
    ids: List[int] = []
    skip = 0
    top = 1000

    while len(ids) < max_ids:
        resp = client.list_ticket_ids_updated_since(since_utc, top=top, skip=skip)
        if resp.status_code >= 400:
            snippet = resp.text[:500].replace("\n", " ")
            print(f"[WARN] fallback: /tickets HTTP {resp.status_code}. Trecho: {snippet}")
            break

        data = resp.json()
        if not isinstance(data, list) or not data:
            break

        for it in data:
            try:
                ids.append(int(it["id"]))
            except Exception:
                continue
            if len(ids) >= max_ids:
                break

        skip += len(data)
        if len(data) < top:
            break

    # agora consulta /tickets/merged por id
    merged_items: List[Dict[str, Any]] = []
    checked = 0
    for tid in ids:
        checked += 1
        resp = client.get_merged_by_id(tid)
        if resp.status_code == 404:
            continue
        if resp.status_code >= 400:
            snippet = resp.text[:200].replace("\n", " ")
            print(f"[WARN] fallback: /tickets/merged?id={tid} HTTP {resp.status_code}. Trecho: {snippet}")
            continue

        try:
            item = resp.json()
            if isinstance(item, dict):
                merged_items.append(item)
        except Exception:
            continue

    print(f"tickets_mesclados: fallback(recent_updates) checou {checked} tickets, encontrou {len(merged_items)} merges.")
    return merged_items


# -----------------------------
# Main
# -----------------------------

def daterange_chunks(start_d: date, end_d: date, chunk_days: int) -> Iterable[Tuple[date, date]]:
    cur = start_d
    while cur <= end_d:
        chunk_end = min(end_d, cur + timedelta(days=chunk_days - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def main():
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("TOKEN")
    if not token:
        print("ERRO: defina MOVIDESK_TOKEN (ou TOKEN).")
        sys.exit(2)

    api_base = os.getenv("MOVIDESK_API_BASE", DEFAULT_API_BASE)
    schema = os.getenv("SYNC_SCHEMA", DEFAULT_SCHEMA)
    table = os.getenv("SYNC_TABLE", DEFAULT_TABLE)
    state_table = os.getenv("SYNC_STATE_TABLE", DEFAULT_STATE_TABLE)

    range_days = int(os.getenv("MERGED_RANGE_DAYS", str(DEFAULT_RANGE_DAYS)))
    overlap_days = int(os.getenv("MERGED_OVERLAP_DAYS", str(DEFAULT_OVERLAP_DAYS)))
    fallback_max_ids = int(os.getenv("MERGED_FALLBACK_MAX_IDS", str(DEFAULT_FALLBACK_MAX_IDS)))

    initial_start_date_env = os.getenv("INITIAL_START_DATE", "").strip()  # YYYY-MM-DD
    now = utcnow()

    conn = pg_connect()
    try:
        ensure_schema_and_tables(conn, schema, table, state_table)

        last_end_s = state_get(conn, schema, state_table, SYNC_KEY_LAST_END)
        if last_end_s:
            try:
                last_end = parse_iso_utc(last_end_s)
            except Exception:
                last_end = now - timedelta(days=7)
        else:
            if initial_start_date_env:
                try:
                    d0 = datetime.strptime(initial_start_date_env, "%Y-%m-%d").date()
                    last_end = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
                except Exception:
                    last_end = now - timedelta(days=7)
            else:
                last_end = now - timedelta(days=7)

        # janela baseada em datas (API trabalha com YYYY-MM-DD)
        start_date = (last_end - timedelta(days=overlap_days)).date()
        end_date = now.date()

        client = MovideskClient(token=token, base_url=api_base)

        total_edges_upserted = 0
        total_items = 0

        print(f"tickets_mesclados: sync principal via /tickets/merged (date-range)")
        print(f"tickets_mesclados: window startDate={to_date_str(start_date)} endDate={to_date_str(end_date)} "
              f"(chunk={range_days}d overlap={overlap_days}d)")

        try:
            for chunk_start, chunk_end in daterange_chunks(start_date, end_date, range_days):
                print(f"tickets_mesclados: chamada /tickets/merged params={{'startDate':'{to_date_str(chunk_start)}','endDate':'{to_date_str(chunk_end)}'}}")

                merged_items = fetch_all_merged_in_range(client, chunk_start, chunk_end)
                total_items += len(merged_items)

                batch_edges: List[EdgeRow] = []
                for it in merged_items:
                    batch_edges.extend(edges_from_merged_item(it))

                # upsert em lotes
                if batch_edges:
                    total_edges_upserted += upsert_edges(conn, schema, table, batch_edges)

        except RuntimeError as e:
            # Se o /tickets/merged por data falhar com 404, fazemos fallback
            msg = str(e)
            print(f"[WARN] sync principal falhou: {msg}")
            print(f"tickets_mesclados: iniciando fallback por updates recentes (via /tickets + /tickets/merged?id=...)")

            since = last_end - timedelta(days=overlap_days)
            merged_items = fallback_by_recent_updates(client, since_utc=since, max_ids=fallback_max_ids)
            total_items += len(merged_items)

            batch_edges: List[EdgeRow] = []
            for it in merged_items:
                batch_edges.extend(edges_from_merged_item(it))

            if batch_edges:
                total_edges_upserted += upsert_edges(conn, schema, table, batch_edges)

        # Atualiza estado (marca sucesso)
        state_set(conn, schema, state_table, SYNC_KEY_LAST_OK, now.isoformat())
        state_set(conn, schema, state_table, SYNC_KEY_LAST_END, now.isoformat())

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            total_in_table = cur.fetchone()[0]

        print(f"tickets_mesclados: itens processados (respostas merged): {total_items}")
        print(f"tickets_mesclados: edges upserted: {total_edges_upserted}")
        print(f"tickets_mesclados: sincronização concluída. Total na tabela: {total_in_table}.")

    except Exception as e:
        # muito importante: rollback pra não deixar transação "aborted"
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[ERRO] falha no sync: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
