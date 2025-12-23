#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_tickets_merged.py

Sincroniza relações de tickets mesclados (merged tickets) do Movidesk para Postgres.

O que este script resolve (pelos seus logs):
- /tickets/merged retornando 404 -> o script tenta e cai em fallback automaticamente.
- 400 ao chamar /tickets com $expand=statusHistories -> fallback NÃO usa $expand.
- erro "column last_update does not exist" + transação abortada -> o script não depende dessa coluna
  e garante rollback/commit corretamente.

Estratégia:
1) Tenta o endpoint dedicado GET /tickets/merged (manual Tickets_Merged).
   Parâmetros relevantes: token (obrigatório), startDate/endDate (UTC, yyyy-MM-ddTHH:mm:ss), page.
2) Se não conseguir (404/erro) OU vier vazio, faz fallback:
   - Varre /tickets e /tickets/past ordenando por lastUpdate desc (OData),
     e dentro da janela lê:
       - mergedTicketsId   (ticket que este ticket foi mesclado em)  -> child -> parent
       - mergedTicketsIds  (tickets mesclados dentro deste ticket)    -> parent -> children
   - Não usa $expand (evita os 400).

Persistência:
schema (default): visualizacao_resolvidos
table  (default): tickets_mesclados

Estrutura (criada se não existir):
  parent_ticket_id  BIGINT NOT NULL
  merged_ticket_id  BIGINT NOT NULL
  last_update_utc   TIMESTAMPTZ NULL
  source            TEXT NULL
  first_seen_utc    TIMESTAMPTZ NOT NULL DEFAULT now()
UNIQUE INDEX: (parent_ticket_id, merged_ticket_id)
(upsert via ON CONFLICT funciona com esse unique index)

ENV suportadas:
- MOVIDESK_TOKEN (obrigatória)  [ou MOVI_TOKEN]
- MOVIDESK_BASE_URL (default: https://api.movidesk.com/public/v1)
- DB:
    * DATABASE_URL  (ex: postgres://user:pass@host:5432/dbname)
      OU
    * PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
- SCHEMA (default: visualizacao_resolvidos)
- TABLE  (default: tickets_mesclados)
- LOOKBACK_MINUTES (default: 90)  -> janela quando START_DATE/END_DATE não são informados
- START_DATE / END_DATE (opcional):
    aceita "YYYY-MM-DD HH:MM:SS" ou ISO; se sem timezone, assume America/Sao_Paulo e converte p/ UTC.
"""

from __future__ import annotations

import os
import re
import time
import math
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.exceptions import HTTPError
import psycopg2
from psycopg2.extras import execute_values

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

SAO_PAULO_TZ = "America/Sao_Paulo"


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v else default


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt_flexible(s: str) -> datetime:
    """
    Aceita:
    - 2025-12-23 11:40:22
    - 2025-12-23T11:40:22
    - 2025-12-23T11:40:22Z
    - 2025-12-23T11:40:22-03:00
    Se não houver timezone, assume America/Sao_Paulo e converte para UTC.
    """
    s = s.strip()
    s = s.replace(" ", "T") if (" " in s and "T" not in s) else s
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            dt = datetime.strptime(s, "%Y-%m-%d")

    if dt.tzinfo is None:
        if ZoneInfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
        else:
            dt = dt.replace(tzinfo=ZoneInfo(SAO_PAULO_TZ))
    return dt.astimezone(timezone.utc)


def _fmt_movidesk_utc(dt_utc: datetime) -> str:
    """
    Manual do Tickets/Merged pede UTC no formato yyyy-MM-ddTHH:mm:ss (sem 'Z').
    """
    dt_utc = dt_utc.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S")


def _safe_int(x: Any) -> Optional[int]:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float) and not math.isnan(x):
        return int(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        s = re.sub(r"[^\d-]", "", s)
        if s in ("", "-"):
            return None
        try:
            return int(s)
        except Exception:
            return None
    return None


def _split_ids(v: Any) -> List[int]:
    """
    mergedTicketsIds pode vir como:
    - string "127230;127229;"
    - string "127230,127229"
    - lista [127230, 127229]
    - lista ["127230", "127229"]
    """
    if v is None:
        return []
    if isinstance(v, list):
        out: List[int] = []
        for it in v:
            n = _safe_int(it)
            if n is not None:
                out.append(n)
        return out
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        parts = re.split(r"[;,]\s*", s)
        out: List[int] = []
        for p in parts:
            n = _safe_int(p)
            if n is not None:
                out.append(n)
        return out
    n = _safe_int(v)
    return [n] if n is not None else []


@dataclass
class Config:
    token: str
    base_url: str
    schema: str
    table: str
    start_utc: datetime
    end_utc: datetime

    # paging / perf
    tickets_top: int = 100

    # retry
    timeout_s: int = 60
    max_retries: int = 5
    backoff_base_s: float = 1.0


class MovideskClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()

    def get_json(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.cfg.base_url.rstrip("/") + "/" + path.lstrip("/")
        params = dict(params)
        params["token"] = self.cfg.token

        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.cfg.timeout_s)

                if resp.status_code == 204:
                    return None

                # transient / rate-limit
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise HTTPError(f"{resp.status_code} {resp.text[:500]}", response=resp)

                if resp.status_code == 404:
                    raise HTTPError(f"404 Not Found: {resp.text[:500]}", response=resp)

                resp.raise_for_status()
                if resp.text.strip() == "":
                    return None
                return resp.json()

            except Exception as e:
                last_exc = e

                status = None
                if isinstance(e, HTTPError) and getattr(e, "response", None) is not None:
                    status = e.response.status_code

                # 4xx que não é 429: não adianta retentar
                if status is not None and status not in (429, 500, 502, 503, 504):
                    raise

                sleep_s = min(self.cfg.backoff_base_s * (2 ** (attempt - 1)), 30.0)
                print(f"[WARN] GET {path} falhou (tentativa {attempt}/{self.cfg.max_retries}): {e}. Retentando em {sleep_s:.1f}s...")
                time.sleep(sleep_s)

        raise RuntimeError(f"GET {path} falhou após {self.cfg.max_retries} tentativas: {last_exc}")


def connect_pg() -> psycopg2.extensions.connection:
    db_url = _env("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    host = _env("PGHOST")
    db = _env("PGDATABASE")
    user = _env("PGUSER")
    pwd = _env("PGPASSWORD")
    port = _env("PGPORT", "5432")

    missing = [k for k, v in [("PGHOST", host), ("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", pwd)] if not v]
    if missing:
        raise RuntimeError(f"Variáveis de banco faltando: {', '.join(missing)} (ou defina DATABASE_URL)")

    return psycopg2.connect(host=host, port=int(port), dbname=db, user=user, password=pwd)


def ensure_db_objects(conn: psycopg2.extensions.connection, schema: str, table: str) -> None:
    """
    Cria schema/tabela/colunas e unique index se necessário.
    """
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {schema};")
        cur.execute(
            f"""
            create table if not exists {schema}.{table} (
                parent_ticket_id bigint not null,
                merged_ticket_id bigint not null,
                last_update_utc timestamptz null,
                source text null,
                first_seen_utc timestamptz not null default now()
            );
            """
        )

        # adiciona colunas caso seja uma tabela antiga
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        need = {
            "parent_ticket_id": "bigint",
            "merged_ticket_id": "bigint",
            "last_update_utc": "timestamptz",
            "source": "text",
            "first_seen_utc": "timestamptz",
        }
        for col, typ in need.items():
            if col not in existing:
                cur.execute(f"alter table {schema}.{table} add column {col} {typ};")

        # unique index para suportar ON CONFLICT
        cur.execute(
            f"""
            create unique index if not exists {table}_uniq_parent_child
            on {schema}.{table} (parent_ticket_id, merged_ticket_id);
            """
        )

    conn.commit()


def parse_merges_from_tickets_merged_payload(payload: Any) -> List[Tuple[int, int, Optional[datetime], str]]:
    """
    Espera algo como (conforme manual):
      {
        "startDate": "...",
        "endDate": "...",
        "count": "2",
        "pageNumber": "1 of 1",
        "tickets": [
          {"ticketId":"127228","mergedTickets":"2","mergedTicketsIds":"127230;127229;","lastUpdate":"2021-06-04 16:00:12"}
        ]
      }
    """
    out: List[Tuple[int, int, Optional[datetime], str]] = []
    if not isinstance(payload, dict):
        return out
    tickets = payload.get("tickets")
    if not isinstance(tickets, list):
        return out

    for t in tickets:
        if not isinstance(t, dict):
            continue
        parent = _safe_int(t.get("ticketId") or t.get("id"))
        if parent is None:
            continue

        merged_ids = _split_ids(t.get("mergedTicketsIds"))

        last_upd: Optional[datetime] = None
        lu = t.get("lastUpdate")
        if isinstance(lu, str) and lu.strip():
            try:
                s = lu.strip().replace(" ", "T")
                dt = datetime.fromisoformat(s)
                last_upd = (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc))
            except Exception:
                last_upd = None

        for child in merged_ids:
            if child != parent:
                out.append((parent, child, last_upd, "tickets/merged"))

    return out


def parse_merges_from_ticket_obj(ticket: Dict[str, Any], source: str) -> List[Tuple[int, int, Optional[datetime], str]]:
    """
    Lê um ticket de /tickets ou /tickets/past e extrai:
    - se mergedTicketsId existe: (parent=mergedTicketsId, child=id)
    - se mergedTicketsIds existe: (parent=id, child in mergedTicketsIds)
    """
    out: List[Tuple[int, int, Optional[datetime], str]] = []

    tid = _safe_int(ticket.get("id") or ticket.get("ticketId"))
    if tid is None:
        return out

    last_upd: Optional[datetime] = None
    lu = ticket.get("lastUpdate")
    if isinstance(lu, str) and lu.strip():
        try:
            last_upd = _parse_dt_flexible(lu)
        except Exception:
            last_upd = None

    # child -> parent
    parent_single = _safe_int(ticket.get("mergedTicketsId"))
    if parent_single is not None and parent_single != tid:
        out.append((parent_single, tid, last_upd, source))

    # parent -> children
    merged_ids = _split_ids(ticket.get("mergedTicketsIds"))
    for child in merged_ids:
        if child != tid:
            out.append((tid, child, last_upd, source))

    return out


def fetch_merged_via_dedicated_endpoint(client: MovideskClient, start_utc: datetime, end_utc: datetime) -> List[Tuple[int, int, Optional[datetime], str]]:
    merges: List[Tuple[int, int, Optional[datetime], str]] = []
    page = 1

    while True:
        params = {
            "startDate": _fmt_movidesk_utc(start_utc),
            "endDate": _fmt_movidesk_utc(end_utc),
            "page": str(page),
        }
        try:
            payload = client.get_json("/tickets/merged", params=params)
        except HTTPError as e:
            status = getattr(e.response, "status_code", None) if getattr(e, "response", None) is not None else None
            print(f"tickets_mesclados: endpoint /tickets/merged retornou {status}.")
            return []

        if not payload:
            break

        page_merges = parse_merges_from_tickets_merged_payload(payload)
        merges.extend(page_merges)

        # tenta inferir totalPages pelo "pageNumber": "1 of 2"
        total_pages = None
        if isinstance(payload, dict) and isinstance(payload.get("pageNumber"), str):
            m = re.search(r"(\d+)\s*of\s*(\d+)", payload["pageNumber"])
            if m:
                total_pages = int(m.group(2))

        if total_pages is not None and page >= total_pages:
            break
        if total_pages is None and not page_merges:
            break

        page += 1
        if page > 2000:
            print("[WARN] /tickets/merged: limite de páginas atingido. Parando por segurança.")
            break

    return merges


def fetch_from_tickets_endpoint(
    client: MovideskClient,
    path: str,
    start_utc: datetime,
    end_utc: datetime,
    top: int,
) -> List[Tuple[int, int, Optional[datetime], str]]:
    """
    Varre /tickets ou /tickets/past ordenando por lastUpdate desc.
    Para quando lastUpdate mais antigo da página ficar < start_utc.
    (evita $filter com datetime, que é onde muitas integrações quebram)
    """
    merges: List[Tuple[int, int, Optional[datetime], str]] = []
    skip = 0
    select_fields = "id,mergedTicketsId,mergedTicketsIds,lastUpdate"

    while True:
        params = {
            "$select": select_fields,
            "$orderby": "lastUpdate desc",
            "$top": str(top),
            "$skip": str(skip),
        }
        payload = client.get_json(path, params=params)
        if not payload:
            break

        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            items = payload["value"]
        elif isinstance(payload, list):
            items = payload
        else:
            print(f"[WARN] {path}: payload inesperado ({type(payload)}). Parando.")
            break

        if not items:
            break

        oldest_dt_in_page: Optional[datetime] = None
        for item in items:
            if not isinstance(item, dict):
                continue

            merges.extend(parse_merges_from_ticket_obj(item, source=path.lstrip("/")))

            lu = item.get("lastUpdate")
            if isinstance(lu, str) and lu.strip():
                try:
                    dt_lu = _parse_dt_flexible(lu)
                except Exception:
                    dt_lu = None
                if dt_lu is not None:
                    oldest_dt_in_page = dt_lu if oldest_dt_in_page is None else min(oldest_dt_in_page, dt_lu)

        if oldest_dt_in_page is not None and oldest_dt_in_page < start_utc:
            break
        if len(items) < top:
            break

        skip += top
        if skip > 50000:
            print(f"[WARN] {path}: limite de paginação atingido (skip={skip}). Parando por segurança.")
            break

    # filtra janela (porque varremos por ordem, mas pode vir algo fora)
    filtered: List[Tuple[int, int, Optional[datetime], str]] = []
    for parent, child, last_upd, src in merges:
        if last_upd is None:
            filtered.append((parent, child, last_upd, src))
        elif start_utc <= last_upd <= end_utc:
            filtered.append((parent, child, last_upd, src))
    return filtered


def upsert_merges(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    rows: List[Tuple[int, int, Optional[datetime], str]],
) -> int:
    """
    rows: (parent_ticket_id, merged_ticket_id, last_update_utc, source)
    """
    if not rows:
        return 0

    # dedup local (mantém last_update mais recente)
    dedup: Dict[Tuple[int, int], Tuple[int, int, Optional[datetime], str]] = {}
    for parent_id, child_id, last_upd, src in rows:
        key = (parent_id, child_id)
        prev = dedup.get(key)
        if prev is None:
            dedup[key] = (parent_id, child_id, last_upd, src)
        else:
            prev_dt = prev[2]
            if prev_dt is None or (last_upd is not None and last_upd > prev_dt):
                dedup[key] = (parent_id, child_id, last_upd, src)

    final_rows = list(dedup.values())

    sql = f"""
    insert into {schema}.{table} (parent_ticket_id, merged_ticket_id, last_update_utc, source)
    values %s
    on conflict (parent_ticket_id, merged_ticket_id) do update
      set last_update_utc = excluded.last_update_utc,
          source = excluded.source
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, final_rows, page_size=500)
    conn.commit()
    return len(final_rows)


def build_config() -> Config:
    token = _env("MOVIDESK_TOKEN") or _env("MOVI_TOKEN")
    if not token:
        raise RuntimeError("Defina MOVIDESK_TOKEN (ou MOVI_TOKEN)")

    base_url = _env("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1")
    schema = _env("SCHEMA", "visualizacao_resolvidos")
    table = _env("TABLE", "tickets_mesclados")

    lookback = _env_int("LOOKBACK_MINUTES", 90)

    end_s = _env("END_DATE")
    start_s = _env("START_DATE")

    end_utc = _parse_dt_flexible(end_s) if end_s else _now_utc()
    start_utc = _parse_dt_flexible(start_s) if start_s else (end_utc - timedelta(minutes=lookback))

    if start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc

    return Config(
        token=token,
        base_url=base_url,
        schema=schema,
        table=table,
        start_utc=start_utc,
        end_utc=end_utc,
    )


def main() -> None:
    cfg = build_config()
    print(f"tickets_mesclados: janela UTC start={_fmt_movidesk_utc(cfg.start_utc)} end={_fmt_movidesk_utc(cfg.end_utc)}")

    client = MovideskClient(cfg)
    conn = connect_pg()

    try:
        ensure_db_objects(conn, cfg.schema, cfg.table)

        print(
            "tickets_mesclados: chamada dedicada /tickets/merged base_params="
            f"{{'startDate': '{_fmt_movidesk_utc(cfg.start_utc)}', 'endDate': '{_fmt_movidesk_utc(cfg.end_utc)}'}}"
        )
        merges = fetch_merged_via_dedicated_endpoint(client, cfg.start_utc, cfg.end_utc)

        if not merges:
            print("tickets_mesclados: usando fallback via /tickets e /tickets/past (sem $expand).")
            merges = []
            try:
                merges.extend(fetch_from_tickets_endpoint(client, "/tickets", cfg.start_utc, cfg.end_utc, top=cfg.tickets_top))
            except Exception as e:
                print(f"[WARN] fallback(/tickets) falhou: {e}")
            try:
                merges.extend(fetch_from_tickets_endpoint(client, "/tickets/past", cfg.start_utc, cfg.end_utc, top=cfg.tickets_top))
            except Exception as e:
                print(f"[WARN] fallback(/tickets/past) falhou: {e}")

        if merges:
            upserts = upsert_merges(conn, cfg.schema, cfg.table, merges)
            print(f"tickets_mesclados: upsert concluído. pares={upserts}.")
        else:
            print("tickets_mesclados: nenhuma relação de merge encontrada na janela.")

        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {cfg.schema}.{cfg.table}")
            total = cur.fetchone()[0]
            print(f"tickets_mesclados: sincronização concluída. Total na tabela: {total}.")

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        print("[ERRO] Falha no sync_tickets_merged.py:")
        traceback.print_exc()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
