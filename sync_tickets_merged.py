#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_tickets_merged.py

Sincroniza relações de "tickets mesclados" do Movidesk para Postgres.

Prioridade:
  1) Endpoint /tickets/merged (conforme manual "Tickets/Merged")
  2) Fallback: varre /tickets e /tickets/past procurando campos de merge
     (mergedTicketId / mergedTicketsIds) em tickets atualizados no período.

Requisitos:
  pip install requests psycopg2-binary

Config por variáveis de ambiente:
  # Movidesk
  MOVIDESK_TOKEN           (obrigatório)
  MOVIDESK_BASE_URL        (opcional; padrão https://api.movidesk.com/public/v1)

  # Postgres (padrão libpq)
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
  DATABASE_URL             (opcional; se presente, usa este DSN)

  # Sync
  SYNC_SCHEMA              (opcional; padrão visualizacao_resolvidos)
  SYNC_TABLE               (opcional; padrão tickets_mesclados)
  STATE_TABLE              (opcional; padrão sync_state)
  LOOKBACK_DAYS            (opcional; padrão 3)  -> usado apenas no primeiro run (sem estado)
  OVERLAP_MINUTES          (opcional; padrão 60) -> reprocessa uma janela anterior (idempotente)
  API_TIMEOUT_SECONDS      (opcional; padrão 60)
  API_PAGE_SIZE            (opcional; padrão 100) -> usado em /tickets e /tickets/past ($top)
  MAX_PAGES_MERGED         (opcional; padrão 2000) -> proteção para /tickets/merged?page=

  # Override manual do período (UTC ISO-8601):
  START_DATE_UTC           (opcional; ex 2025-12-23T00:00:00Z)
  END_DATE_UTC             (opcional; ex 2025-12-23T23:59:59Z)

Tabela destino (default):
  visualizacao_resolvidos.tickets_mesclados
    - ticket_id          BIGINT   (ticket "principal", que recebeu a mesclagem)
    - merged_ticket_id   BIGINT   (ticket mesclado dentro do principal)
    - last_update        TIMESTAMPTZ
    - source             TEXT     ('tickets_merged', 'tickets', 'tickets_past')
    - raw               JSONB     (payload mínimo da origem)
    - inserted_at        TIMESTAMPTZ (default now())
    - updated_at         TIMESTAMPTZ (default now())

Chave única: (ticket_id, merged_ticket_id)
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# ----------------------------
# Logging
# ----------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tickets_mesclados")


# ----------------------------
# Helpers: datetime parsing/formatting
# ----------------------------

def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO-8601 / Movidesk-like datetimes safely -> aware datetime (UTC if Z)."""
    if not value:
        return None
    s = value.strip()
    # Normalize "Z" to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Some APIs return "YYYY-MM-DD HH:MM:SS" (sem T)
    # Convert to ISO if needed
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # fallback a formatos comuns
        for fmt in ("%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            return None
    if dt.tzinfo is None:
        # Se vier sem tz, assume UTC (manual menciona UTC)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_utc_z(dt: datetime) -> str:
    """Format datetime as UTC ISO-8601 with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class Config:
    movidesk_token: str
    base_url: str

    pg_dsn: str

    schema: str
    table: str
    state_table: str

    lookback_days: int
    overlap_minutes: int

    timeout_s: int
    page_size: int
    max_pages_merged: int

    override_start: Optional[datetime]
    override_end: Optional[datetime]


def load_config() -> Config:
    token = os.getenv("MOVIDESK_TOKEN", "").strip()
    if not token:
        raise SystemExit("MOVIDESK_TOKEN não definido.")

    base_url = os.getenv("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1").rstrip("/")

    # Postgres DSN
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        db = os.getenv("PGDATABASE", "postgres")
        user = os.getenv("PGUSER", "postgres")
        pwd = os.getenv("PGPASSWORD", "")
        dsn = f"host={host} port={port} dbname={db} user={user} password={pwd}"

    schema = os.getenv("SYNC_SCHEMA", "visualizacao_resolvidos")
    table = os.getenv("SYNC_TABLE", "tickets_mesclados")
    state_table = os.getenv("STATE_TABLE", "sync_state")

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "3"))
    overlap_minutes = int(os.getenv("OVERLAP_MINUTES", "60"))

    timeout_s = int(os.getenv("API_TIMEOUT_SECONDS", "60"))
    page_size = int(os.getenv("API_PAGE_SIZE", "100"))
    max_pages_merged = int(os.getenv("MAX_PAGES_MERGED", "2000"))

    override_start = parse_iso_datetime(os.getenv("START_DATE_UTC"))
    override_end = parse_iso_datetime(os.getenv("END_DATE_UTC"))

    if override_start and not override_end:
        override_end = now_utc()
    if override_end and not override_start:
        override_start = override_end - timedelta(days=1)

    return Config(
        movidesk_token=token,
        base_url=base_url,
        pg_dsn=dsn,
        schema=schema,
        table=table,
        state_table=state_table,
        lookback_days=lookback_days,
        overlap_minutes=overlap_minutes,
        timeout_s=timeout_s,
        page_size=page_size,
        max_pages_merged=max_pages_merged,
        override_start=override_start,
        override_end=override_end,
    )


# ----------------------------
# DB
# ----------------------------

def connect_db(cfg: Config):
    conn = psycopg2.connect(cfg.pg_dsn)
    conn.autocommit = False
    return conn


def ensure_schema_and_tables(conn, cfg: Config) -> None:
    """Cria schema/tabelas e faz migrações leves (adiciona colunas se faltarem)."""
    create_schema_sql = f"create schema if not exists {cfg.schema};"
    create_state_sql = f"""
        create table if not exists {cfg.schema}.{cfg.state_table} (
            key text primary key,
            value text not null,
            updated_at timestamptz not null default now()
        );
    """
    create_main_sql = f"""
        create table if not exists {cfg.schema}.{cfg.table} (
            ticket_id bigint not null,
            merged_ticket_id bigint not null,
            last_update timestamptz null,
            source text null,
            raw jsonb null,
            inserted_at timestamptz not null default now(),
            updated_at timestamptz not null default now(),
            constraint {cfg.table}_pk primary key (ticket_id, merged_ticket_id)
        );
    """

    with conn.cursor() as cur:
        cur.execute(create_schema_sql)
        cur.execute(create_state_sql)
        cur.execute(create_main_sql)

        cur.execute(
            """
            select column_name
              from information_schema.columns
             where table_schema = %s and table_name = %s;
            """,
            (cfg.schema, cfg.table),
        )
        cols = {r[0] for r in cur.fetchall()}

        # Migração: se existir lastupdate sem last_update
        if "lastupdate" in cols and "last_update" not in cols:
            cur.execute(f"alter table {cfg.schema}.{cfg.table} add column last_update timestamptz null;")
            cur.execute(f"update {cfg.schema}.{cfg.table} set last_update = lastupdate where last_update is null;")

        # Garante colunas esperadas
        for col, ddl in [
            ("source", "text null"),
            ("raw", "jsonb null"),
            ("inserted_at", "timestamptz not null default now()"),
            ("updated_at", "timestamptz not null default now()"),
        ]:
            if col not in cols:
                cur.execute(f"alter table {cfg.schema}.{cfg.table} add column {col} {ddl};")

    conn.commit()


def get_state(conn, cfg: Config, key: str) -> Optional[str]:
    sql = f"select value from {cfg.schema}.{cfg.state_table} where key=%s;"
    with conn.cursor() as cur:
        cur.execute(sql, (key,))
        row = cur.fetchone()
    return row[0] if row else None


def set_state(conn, cfg: Config, key: str, value: str) -> None:
    sql = f"""
        insert into {cfg.schema}.{cfg.state_table}(key, value)
        values (%s, %s)
        on conflict (key) do update
           set value=excluded.value, updated_at=now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (key, value))
    conn.commit()


def upsert_edges(
    conn,
    cfg: Config,
    edges: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any]]],
) -> int:
    """
    edges: list of (ticket_id, merged_ticket_id, last_update_dt, source, raw_dict)
    """
    if not edges:
        return 0

    records = []
    for ticket_id, merged_ticket_id, last_update, source, raw in edges:
        records.append((
            int(ticket_id),
            int(merged_ticket_id),
            last_update,
            source,
            json.dumps(raw, ensure_ascii=False),
        ))

    sql = f"""
        insert into {cfg.schema}.{cfg.table}
            (ticket_id, merged_ticket_id, last_update, source, raw, updated_at)
        values %s
        on conflict (ticket_id, merged_ticket_id) do update
            set last_update = greatest({cfg.schema}.{cfg.table}.last_update, excluded.last_update),
                source = excluded.source,
                raw = excluded.raw,
                updated_at = now();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql,
            records,
            template="(%s,%s,%s,%s,%s::jsonb,now())",
            page_size=1000,
        )
    conn.commit()
    return len(records)


def count_rows(conn, cfg: Config) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {cfg.schema}.{cfg.table};")
        return int(cur.fetchone()[0])


# ----------------------------
# Movidesk API client
# ----------------------------

class MovideskClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _request(self, path: str, params: Dict[str, Any], max_retries: int = 5) -> requests.Response:
        url = f"{self.cfg.base_url}/{path.lstrip('/')}"
        merged_params = dict(params)
        merged_params["token"] = self.cfg.movidesk_token

        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, params=merged_params, timeout=self.cfg.timeout_s)
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(2 ** (attempt - 1), 30)
                    log.warning("HTTP %s em %s (tentativa %s/%s). Aguardando %ss.",
                                resp.status_code, path, attempt, max_retries, wait)
                    time.sleep(wait)
                    continue
                return resp
            except requests.RequestException as e:
                last_err = e
                wait = min(2 ** (attempt - 1), 30)
                log.warning("Erro de rede em %s (tentativa %s/%s): %s. Aguardando %ss.",
                            path, attempt, max_retries, e, wait)
                time.sleep(wait)

        raise RuntimeError(f"Falha ao chamar {path}: {last_err}")

    # ---- /tickets/merged ----

    def fetch_tickets_merged_by_date(
        self,
        start_utc: datetime,
        end_utc: datetime,
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Tenta usar /tickets/merged (manual).
        Retorna (supported, items).
          - supported=False quando o endpoint retornar 404 (path não existe).
        """
        all_items: List[Dict[str, Any]] = []
        page = 1
        for _ in range(self.cfg.max_pages_merged):
            params = {
                "startDate": to_utc_z(start_utc),
                "endDate": to_utc_z(end_utc),
                "page": page,
            }
            log.info("tickets_mesclados: chamada dedicada /tickets/merged params=%s", {k: params[k] for k in params})
            resp = self._request("/tickets/merged", params=params)
            if resp.status_code == 400:
                # Alguns tenants aceitam 'YYYY-MM-DD HH:MM:SS' (UTC) em vez de ISO-8601
                alt_params = dict(params)
                alt_params["startDate"] = start_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                alt_params["endDate"] = end_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                resp = self._request("/tickets/merged", params=alt_params)

            if resp.status_code == 404:
                log.warning("tickets_mesclados: endpoint /tickets/merged retornou 404.")
                return (False, [])
            if resp.status_code == 204:
                return (True, [])
            if not resp.ok:
                log.warning("tickets_mesclados: /tickets/merged retornou HTTP %s. Body: %s",
                            resp.status_code, _safe_body(resp))
                return (True, [])

            try:
                payload = resp.json()
            except Exception:
                log.warning("tickets_mesclados: JSON inválido em /tickets/merged. Body: %s", _safe_body(resp))
                return (True, [])

            items = payload.get("items") if isinstance(payload, dict) else payload
            if not items:
                break
            if not isinstance(items, list):
                log.warning("tickets_mesclados: formato inesperado em /tickets/merged: %s", type(items))
                break

            all_items.extend(items)

            total_pages = None
            if isinstance(payload, dict):
                total_pages = payload.get("totalPages") or payload.get("pages") or payload.get("TotalPages")
            if total_pages and page >= int(total_pages):
                break

            page += 1

        return (True, all_items)

    # ---- /tickets and /tickets/past ----

    def fetch_updated_tickets(
        self,
        endpoint: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Busca tickets atualizados no intervalo via OData.
        endpoint: '/tickets' ou '/tickets/past'
        """
        items: List[Dict[str, Any]] = []
        skip = 0
        top = self.cfg.page_size

        start_s = to_utc_z(start_utc)
        end_s = to_utc_z(end_utc)

        base_filter_variants = [
            f"lastUpdate ge {start_s} and lastUpdate le {end_s}",
            f"lastUpdate ge '{start_s}' and lastUpdate le '{end_s}'",
            f"lastUpdate ge datetime'{start_s}' and lastUpdate le datetime'{end_s}'",
        ]

        select_variants = [
            "id,lastUpdate,mergedTicketId,mergedTicketsIds",
            "id,lastUpdate,mergedTicketId",
            "id,lastUpdate,mergedTicketsIds",
            None,
        ]

        working_filter = None
        working_select = None

        for fexp in base_filter_variants:
            for sel in select_variants:
                test_params = {"$filter": fexp, "$top": 1, "$skip": 0}
                if sel:
                    test_params["$select"] = sel
                resp = self._request(endpoint, test_params)
                if resp.status_code == 404:
                    log.warning("Endpoint %s retornou 404. Pulando.", endpoint)
                    return []
                if resp.status_code == 400:
                    continue
                if not resp.ok:
                    log.warning("HTTP %s em %s (probe). Body: %s", resp.status_code, endpoint, _safe_body(resp))
                    continue
                working_filter = fexp
                working_select = sel
                break
            if working_filter:
                break

        if not working_filter:
            log.warning("Não foi possível montar query válida para %s com filtro lastUpdate.", endpoint)
            return []

        while True:
            params: Dict[str, Any] = {"$filter": working_filter, "$top": top, "$skip": skip}
            if working_select:
                params["$select"] = working_select

            resp = self._request(endpoint, params)
            if not resp.ok:
                log.warning("Erro HTTP ao buscar %s skip=%s top=%s: %s Body: %s",
                            endpoint, skip, top, resp.status_code, _safe_body(resp))
                break

            try:
                data = resp.json()
            except Exception:
                log.warning("JSON inválido em %s. Body: %s", endpoint, _safe_body(resp))
                break

            batch = data.get("value") if isinstance(data, dict) and "value" in data else data
            if not batch:
                break
            if not isinstance(batch, list):
                log.warning("Formato inesperado em %s: %s", endpoint, type(batch))
                break

            items.extend(batch)
            if len(batch) < top:
                break
            skip += top

        log.info("%s: coletados %s tickets no intervalo.", endpoint, len(items))
        return items


def _safe_body(resp: requests.Response, limit: int = 400) -> str:
    try:
        t = resp.text or ""
    except Exception:
        return ""
    t = t.strip().replace("\n", " ")
    return t[:limit]


# ----------------------------
# Transform: API -> edges
# ----------------------------

def normalize_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        return int(value)
    except Exception:
        return None


def extract_edges_from_tickets_merged(items: List[Dict[str, Any]]) -> List[Tuple[int, int, Optional[datetime], str, Dict[str, Any]]]:
    edges = []
    for it in items:
        ticket_id = normalize_int(it.get("ticketId") or it.get("id"))
        if not ticket_id:
            continue
        last_update = parse_iso_datetime(it.get("lastUpdate"))
        merged_ids = it.get("mergedTicketsIds") or []
        if isinstance(merged_ids, str):
            merged_ids = [x.strip() for x in merged_ids.split(",") if x.strip()]
        if not isinstance(merged_ids, list):
            merged_ids = []
        for mid in merged_ids:
            mid_i = normalize_int(mid)
            if not mid_i:
                continue
            edges.append((ticket_id, mid_i, last_update, "tickets_merged", it))
    return edges


def extract_edges_from_ticket_item(ticket: Dict[str, Any], source: str) -> List[Tuple[int, int, Optional[datetime], str, Dict[str, Any]]]:
    """
    Interpreta possíveis campos de merge em /tickets e /tickets/past.
      - Se "mergedTicketId" existir: parent = mergedTicketId, child = id
      - Se "mergedTicketsIds" existir: parent = id, children = mergedTicketsIds
    """
    edges = []
    tid = normalize_int(ticket.get("id") or ticket.get("ticketId"))
    if not tid:
        return edges

    last_update = parse_iso_datetime(ticket.get("lastUpdate"))

    merged_into = (
        ticket.get("mergedTicketId")
        or ticket.get("mergedTicketsId")
        or ticket.get("mergedIntoTicketId")
    )
    merged_into_i = normalize_int(merged_into)
    if merged_into_i:
        edges.append((merged_into_i, tid, last_update, source, {"id": tid, "mergedTicketId": merged_into_i, "lastUpdate": ticket.get("lastUpdate")}))

    merged_children = ticket.get("mergedTicketsIds") or ticket.get("mergedTicketsIdS") or []
    if isinstance(merged_children, str):
        merged_children = [x.strip() for x in merged_children.split(",") if x.strip()]
    if isinstance(merged_children, list):
        for child in merged_children:
            child_i = normalize_int(child)
            if child_i:
                edges.append((tid, child_i, last_update, source, {"id": tid, "mergedTicketsIds": merged_children, "lastUpdate": ticket.get("lastUpdate")}))

    return edges


# ----------------------------
# Main sync
# ----------------------------

STATE_KEY_LAST_RUN = "tickets_merged_last_run_utc"


def compute_window(conn, cfg: Config) -> Tuple[datetime, datetime]:
    if cfg.override_start and cfg.override_end:
        start = cfg.override_start
        end = cfg.override_end
        log.info("Período forçado por env: %s -> %s", to_utc_z(start), to_utc_z(end))
        return start, end

    last_run_s = get_state(conn, cfg, STATE_KEY_LAST_RUN)
    last_run = parse_iso_datetime(last_run_s) if last_run_s else None

    end = now_utc()
    if last_run:
        start = last_run - timedelta(minutes=cfg.overlap_minutes)
    else:
        start = end - timedelta(days=cfg.lookback_days)

    if start >= end:
        start = end - timedelta(minutes=max(cfg.overlap_minutes, 5))

    return start, end


def main() -> None:
    cfg = load_config()
    conn = connect_db(cfg)
    try:
        ensure_schema_and_tables(conn, cfg)
    except Exception:
        conn.rollback()
        raise

    client = MovideskClient(cfg)

    start_utc, end_utc = compute_window(conn, cfg)
    log.info("tickets_mesclados: janela UTC %s -> %s", to_utc_z(start_utc), to_utc_z(end_utc))

    total_inserted = 0
    edges: List[Tuple[int, int, Optional[datetime], str, Dict[str, Any]]] = []

    # 1) Tenta /tickets/merged
    supported, merged_items = client.fetch_tickets_merged_by_date(start_utc, end_utc)
    if supported and merged_items:
        edges = extract_edges_from_tickets_merged(merged_items)
        log.info("tickets_mesclados: /tickets/merged retornou %s registros; %s relações extraídas.",
                 len(merged_items), len(edges))
        try:
            total_inserted += upsert_edges(conn, cfg, edges)
        except Exception as e:
            conn.rollback()
            log.error("Falha ao gravar edges de /tickets/merged: %s", e)
            raise

    # 2) Fallback
    if (not supported) or (not merged_items):
        try:
            tickets = client.fetch_updated_tickets("/tickets", start_utc, end_utc)
            for t in tickets:
                edges.extend(extract_edges_from_ticket_item(t, "tickets"))
        except Exception as e:
            log.warning("Fallback /tickets falhou: %s", e)

        try:
            tickets_past = client.fetch_updated_tickets("/tickets/past", start_utc, end_utc)
            for t in tickets_past:
                edges.extend(extract_edges_from_ticket_item(t, "tickets_past"))
        except Exception as e:
            log.warning("Fallback /tickets/past falhou: %s", e)

        # Dedup
        uniq = {}
        for ticket_id, merged_ticket_id, last_update, source, raw in edges:
            key = (ticket_id, merged_ticket_id)
            prev = uniq.get(key)
            if not prev:
                uniq[key] = (ticket_id, merged_ticket_id, last_update, source, raw)
            else:
                prev_last = prev[2] or datetime.min.replace(tzinfo=timezone.utc)
                cur_last = last_update or datetime.min.replace(tzinfo=timezone.utc)
                if cur_last >= prev_last:
                    uniq[key] = (ticket_id, merged_ticket_id, last_update, source, raw)
        edges = list(uniq.values())

        log.info("tickets_mesclados: fallback extraiu %s relações.", len(edges))

        if edges:
            try:
                total_inserted += upsert_edges(conn, cfg, edges)
            except Exception as e:
                conn.rollback()
                log.error("Falha ao gravar edges do fallback: %s", e)
                raise
        else:
            log.info("tickets_mesclados: fallback não identificou merges no período.")

    # Atualiza estado
    try:
        set_state(conn, cfg, STATE_KEY_LAST_RUN, to_utc_z(end_utc))
    except Exception as e:
        conn.rollback()
        log.warning("Falha ao atualizar state: %s", e)

    total = count_rows(conn, cfg)
    log.info("tickets_mesclados: sincronização concluída. Inseridos/atualizados nesta execução: %s. Total na tabela: %s.",
             total_inserted, total)

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("Erro fatal: %s", exc)
        sys.exit(1)
