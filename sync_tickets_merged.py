#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# -------------------------
# Config / Utils
# -------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"[WARN] {msg}", flush=True)

def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", flush=True)
    sys.exit(code)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso8601_z(s: str) -> Optional[datetime]:
    # Ex: 2025-12-23T13:02:44Z
    try:
        if s.endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        return None

def format_iso8601_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def format_merged_param(dt: datetime) -> str:
    # Manual mostra startDate/endDate como "YYYY-MM-DD" (e às vezes aparece datetime em exemplos).
    # Usar datetime dá granularidade melhor sem quebrar quem aceita; se o endpoint existir e só aceitar data,
    # ele normalmente ainda aceita "YYYY-MM-DD". Ajuste aqui se quiser forçar só data.
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def chunked(lst: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(lst), size):
        yield lst[i:i+size]


# -------------------------
# Movidesk Client
# -------------------------

@dataclass
class MovideskClient:
    base_url: str
    token: str
    timeout: int = 60
    max_retries: int = 5
    backoff_base: float = 1.5

    def get(self, path: str, params: Dict[str, Any]) -> requests.Response:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        params = dict(params)
        params["token"] = self.token

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.get(url, params=params, timeout=self.timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    wait = (self.backoff_base ** (attempt - 1)) + (0.1 * attempt)
                    warn(f"HTTP {r.status_code} em GET {path}. retry {attempt}/{self.max_retries} em {wait:.1f}s")
                    time.sleep(wait)
                    continue
                return r
            except Exception as e:
                last_exc = e
                wait = (self.backoff_base ** (attempt - 1)) + (0.1 * attempt)
                warn(f"exceção em GET {path}: {e}. retry {attempt}/{self.max_retries} em {wait:.1f}s")
                time.sleep(wait)

        raise RuntimeError(f"falha após retries em GET {path}: {last_exc}")


# -------------------------
# Database
# -------------------------

def pg_connect():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        return psycopg2.connect(db_url)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")

    if not (host and db and user):
        die("Defina DATABASE_URL ou (PGHOST, PGDATABASE, PGUSER, PGPASSWORD).")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


def ensure_schema_and_tables(conn, schema: str) -> None:
    ddl = f"""
    create schema if not exists {schema};

    create table if not exists {schema}.sync_state (
        key text primary key,
        last_run_end timestamptz,
        updated_at timestamptz not null default now()
    );

    create table if not exists {schema}.tickets_mesclados (
        ticket_id bigint not null,
        merged_ticket_id bigint not null,
        source text not null,
        last_update timestamptz,
        inserted_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        primary key (ticket_id, merged_ticket_id)
    );

    create index if not exists idx_tickets_mesclados_ticket_id
        on {schema}.tickets_mesclados(ticket_id);

    create index if not exists idx_tickets_mesclados_merged_ticket_id
        on {schema}.tickets_mesclados(merged_ticket_id);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def get_last_run_end(conn, schema: str, key: str) -> Optional[datetime]:
    sql = f"select last_run_end from {schema}.sync_state where key = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (key,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


def set_last_run_end(conn, schema: str, key: str, end_dt: datetime) -> None:
    sql = f"""
    insert into {schema}.sync_state(key, last_run_end, updated_at)
    values (%s, %s, now())
    on conflict (key) do update
      set last_run_end = excluded.last_run_end,
          updated_at = now()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (key, end_dt))
    conn.commit()


def upsert_relations(
    conn,
    schema: str,
    rows: List[Tuple[int, int, str, Optional[datetime]]],
) -> int:
    """
    rows: (ticket_id, merged_ticket_id, source, last_update)
    """
    if not rows:
        return 0

    sql = f"""
    insert into {schema}.tickets_mesclados
      (ticket_id, merged_ticket_id, source, last_update, updated_at)
    values %s
    on conflict (ticket_id, merged_ticket_id) do update
      set source = excluded.source,
          last_update = (
            case
              when excluded.last_update is null then {schema}.tickets_mesclados.last_update
              when {schema}.tickets_mesclados.last_update is null then excluded.last_update
              else greatest({schema}.tickets_mesclados.last_update, excluded.last_update)
            end
          ),
          updated_at = now()
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def count_total(conn, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {schema}.tickets_mesclados")
        return int(cur.fetchone()[0])


# -------------------------
# Sync Logic
# -------------------------

def sync_via_tickets_merged(
    client: MovideskClient,
    conn,
    schema: str,
    start_dt: datetime,
    end_dt: datetime,
) -> Tuple[bool, int]:
    """
    Retorna (ok, inserted_count).
    ok=False se endpoint não existir (404) ou outro erro impeditivo.
    """
    inserted = 0
    start_param = format_merged_param(start_dt)
    end_param = format_merged_param(end_dt)

    log(f"tickets_mesclados: chamada dedicada /tickets/merged base_params={{'startDate': '{start_param}', 'endDate': '{end_param}'}}")

    page = 1
    total_pages: Optional[int] = None

    while True:
        params = {
            "startDate": start_param,
            "endDate": end_param,
            "page": page,
        }
        r = client.get("/tickets/merged", params=params)

        if r.status_code == 404:
            log("tickets_mesclados: endpoint /tickets/merged retornou 404.")
            return (False, inserted)

        if r.status_code >= 400:
            warn(f"/tickets/merged falhou HTTP {r.status_code}: {r.text[:500]}")
            return (False, inserted)

        try:
            data = r.json()
        except Exception:
            warn(f"/tickets/merged retornou JSON inválido. body[:300]={r.text[:300]}")
            return (False, inserted)

        merged_tickets = data.get("mergedTickets") or []
        page_info = (data.get("pageNumber") or "").strip()

        # pageNumber vem como "1 of 333"
        m = re.search(r"(\d+)\s+of\s+(\d+)", page_info, flags=re.IGNORECASE)
        if m:
            total_pages = int(m.group(2))

        rows: List[Tuple[int, int, str, Optional[datetime]]] = []
        for item in merged_tickets:
            try:
                ticket_id = int(item.get("ticketId"))
            except Exception:
                continue

            ids_raw = (item.get("mergedTicketsIds") or "").strip()
            last_update_str = (item.get("lastUpdate") or "").strip()
            last_update_dt: Optional[datetime] = None
            # lastUpdate aqui costuma vir "YYYY-MM-DD HH:MM:SS"
            if last_update_str:
                try:
                    last_update_dt = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    last_update_dt = None

            if not ids_raw:
                continue

            # "1;2;3"
            for part in ids_raw.split(";"):
                part = part.strip()
                if not part:
                    continue
                try:
                    merged_id = int(part)
                except Exception:
                    continue
                if merged_id == ticket_id:
                    continue
                rows.append((ticket_id, merged_id, "tickets/merged", last_update_dt))

        if rows:
            inserted += upsert_relations(conn, schema, rows)

        # Critério de parada
        if total_pages is not None:
            if page >= total_pages:
                break
            page += 1
            continue

        # Se não veio pageNumber confiável, para quando a lista vier vazia
        if not merged_tickets:
            break
        page += 1

    return (True, inserted)


def iter_tickets_by_lastupdate(
    client: MovideskClient,
    endpoint: str,
    start_dt: datetime,
    end_dt: datetime,
    top: int = 100,
) -> Iterable[Dict[str, Any]]:
    """
    Busca tickets paginando com $top/$skip filtrando por lastUpdate.
    endpoint: "/tickets" ou "/tickets/past"
    """
    skip = 0
    start_z = format_iso8601_z(start_dt)
    end_z = format_iso8601_z(end_dt)

    # Exemplo do material: createdDate ge 2019-04-11T00:00:00Z (sem aspas).
    odata_filter = f"lastUpdate ge {start_z} and lastUpdate le {end_z}"

    select_fields = "id,lastUpdate,parentTickets,childrenTickets"

    while True:
        params = {
            "$select": select_fields,
            "$filter": odata_filter,
            "$top": str(top),
            "$skip": str(skip),
        }
        r = client.get(endpoint, params=params)
        if r.status_code >= 400:
            raise requests.HTTPError(f"{r.status_code} {r.text[:300]}", response=r)

        data = r.json()
        if not isinstance(data, list):
            # Em geral a API retorna lista; se vier objeto, tenta adaptar
            data = data.get("value") or []

        if not data:
            break

        for item in data:
            if isinstance(item, dict):
                yield item

        if len(data) < top:
            break

        skip += top


def extract_relations_from_ticket(ticket: Dict[str, Any], source: str) -> List[Tuple[int, int, str, Optional[datetime]]]:
    """
    Usa parentTickets/childrenTickets para montar relações (ticket_id -> merged_ticket_id).
    Interpretação prática:
      - Se o ticket tem childrenTickets, assume que aqueles IDs foram "absorvidos" pelo ticket (ticket = pai)
      - Se o ticket tem parentTickets, assume que este ticket foi para dentro do(s) pai(s)
    """
    rows: List[Tuple[int, int, str, Optional[datetime]]] = []

    try:
        tid = int(ticket.get("id"))
    except Exception:
        return rows

    last_update_dt: Optional[datetime] = None
    lu = ticket.get("lastUpdate")
    if isinstance(lu, str) and lu:
        # normalmente vem ISO Z
        parsed = parse_iso8601_z(lu)
        if parsed:
            last_update_dt = parsed.astimezone(timezone.utc)

    children = ticket.get("childrenTickets") or []
    if isinstance(children, list):
        for ch in children:
            if not isinstance(ch, dict):
                continue
            try:
                mid = int(ch.get("id"))
            except Exception:
                continue
            if mid == tid:
                continue
            rows.append((tid, mid, source, last_update_dt))

    parents = ticket.get("parentTickets") or []
    if isinstance(parents, list):
        for p in parents:
            if not isinstance(p, dict):
                continue
            try:
                pid = int(p.get("id"))
            except Exception:
                continue
            if pid == tid:
                continue
            # pai -> este ticket
            rows.append((pid, tid, source, last_update_dt))

    return rows


def sync_fallback_from_tickets(
    client: MovideskClient,
    conn,
    schema: str,
    start_dt: datetime,
    end_dt: datetime,
) -> int:
    """
    Fallback quando /tickets/merged não existe:
      - varre /tickets e /tickets/past por lastUpdate dentro da janela
      - extrai relações via parentTickets/childrenTickets
    """
    inserted = 0

    for endpoint in ("/tickets", "/tickets/past"):
        fetched = 0
        collected: List[Tuple[int, int, str, Optional[datetime]]] = []
        try:
            for t in iter_tickets_by_lastupdate(client, endpoint, start_dt, end_dt, top=100):
                fetched += 1
                rows = extract_relations_from_ticket(t, source=endpoint.lstrip("/"))
                if rows:
                    collected.extend(rows)

                # flush em lotes
                if len(collected) >= 2000:
                    inserted += upsert_relations(conn, schema, collected)
                    collected.clear()

            if collected:
                inserted += upsert_relations(conn, schema, collected)
                collected.clear()

            log(f"tickets_mesclados: fallback({endpoint}) processou {fetched} tickets e gravou {inserted} relações (acumulado).")
        except requests.HTTPError as e:
            warn(f"erro HTTP no fallback {endpoint}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        except Exception as e:
            warn(f"erro inesperado no fallback {endpoint}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    return inserted


# -------------------------
# Main
# -------------------------

def main() -> None:
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN") or os.getenv("TOKEN")
    if not token:
        die("Defina MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN).")

    base_url = os.getenv("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
    schema = os.getenv("PGSCHEMA", "visualizacao_resolvidos").strip()
    state_key = "tickets_mesclados"

    conn = pg_connect()
    conn.autocommit = False

    ensure_schema_and_tables(conn, schema)

    # Janela de sync
    now = utc_now()
    default_lookback_hours = int(os.getenv("DEFAULT_LOOKBACK_HOURS", "24"))
    overlap_seconds = int(os.getenv("OVERLAP_SECONDS", "120"))

    last_end = get_last_run_end(conn, schema, state_key)

    # Permite override manual por env
    env_start = os.getenv("START_DATE", "").strip()  # aceita ISO "2025-12-23T00:00:00Z"
    env_end = os.getenv("END_DATE", "").strip()

    if env_end:
        end_dt = parse_iso8601_z(env_end) or now
    else:
        end_dt = now

    if env_start:
        start_dt = parse_iso8601_z(env_start)
        if not start_dt:
            die("START_DATE inválido. Use formato tipo 2025-12-23T00:00:00Z")
    else:
        if last_end:
            start_dt = last_end - timedelta(seconds=overlap_seconds)
        else:
            start_dt = now - timedelta(hours=default_lookback_hours)

    # sanity
    if start_dt >= end_dt:
        warn(f"janela inválida start>=end ({start_dt} >= {end_dt}). ajustando start para end-1h.")
        start_dt = end_dt - timedelta(hours=1)

    client = MovideskClient(base_url=base_url, token=token)

    # 1) tenta /tickets/merged (manual)
    ok, inserted = (False, 0)
    try:
        ok, inserted = sync_via_tickets_merged(client, conn, schema, start_dt, end_dt)
    except Exception as e:
        warn(f"falha inesperada em /tickets/merged: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        ok = False

    # 2) fallback (quando não existe /tickets/merged)
    if not ok:
        inserted += sync_fallback_from_tickets(client, conn, schema, start_dt, end_dt)

    # Atualiza estado
    set_last_run_end(conn, schema, state_key, end_dt)

    total = count_total(conn, schema)
    log(f"tickets_mesclados: sincronização concluída. Inseridos/atualizados nesta execução: {inserted}. Total na tabela: {total}.")

    conn.close()


if __name__ == "__main__":
    main()
