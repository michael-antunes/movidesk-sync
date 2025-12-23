#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync de /tickets/merged (Movidesk) para Postgres.

Este script segue o manual "API do Movidesk - Tickets_Merged":
- Endpoint: GET /public/v1/tickets/merged
- Consulta por Ticket: parâmetro `id`
- Consulta por período: parâmetros `startDate` e `endDate` (formato yyyy-MM-dd)
- Paginação: parâmetro `page` (retorno traz `pageNumber` tipo "1 of 333")

Requisitos (pip):
  pip install requests psycopg2-binary

Variáveis de ambiente:
  MOVIDESK_TOKEN            (obrigatório)
  MOVIDESK_API_BASE         (opcional) default: https://api.movidesk.com/public/v1

  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (obrigatório para Postgres)

  MOVIDESK_MERGED_SCHEMA    (opcional) default: visualizacao_resolvidos
  MOVIDESK_MERGED_TABLE     (opcional) default: tickets_mesclados

  START_DATE                (opcional) formato yyyy-MM-dd
  END_DATE                  (opcional) formato yyyy-MM-dd
  WINDOW_DAYS               (opcional) default: 7

  IGNORE_MERGED_404         (opcional) "1" para não falhar se /tickets/merged retornar 404
"""

from __future__ import annotations

import os
import sys
import time
import json
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


TZ = ZoneInfo("America/Sao_Paulo")


def log(msg: str) -> None:
    print(msg, flush=True)


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def parse_date_yyyy_mm_dd(s: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception as e:
        raise ValueError(f"Data inválida para {s!r}. Use yyyy-MM-dd.") from e


def parse_dt_last_update(s: str) -> Optional[datetime]:
    """
    lastUpdate no manual aparece como "yyyy-MM-dd HH:mm:ss".
    Aceita também ISO-8601 por segurança.
    """
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # tenta ISO com timezone
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ).replace(tzinfo=None)
    except Exception:
        return None


@dataclass
class MovideskClient:
    base_url: str
    token: str
    timeout: int = 60
    max_retries: int = 6

    def get(self, path: str, params: Dict[str, Any]) -> requests.Response:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        # token como query param (padrão Movidesk)
        params = dict(params)
        params["token"] = self.token

        backoff = 1.0
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.get(url, params=params, timeout=self.timeout)
                # 429/5xx: retry
                if r.status_code in (429, 500, 502, 503, 504):
                    log(f"[WARN] HTTP {r.status_code} em {url} (tentativa {attempt}/{self.max_retries}). Retentando…")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                return r
            except requests.RequestException as e:
                last_exc = e
                log(f"[WARN] erro de rede em {url} (tentativa {attempt}/{self.max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

        raise RuntimeError(f"Falha após {self.max_retries} tentativas em {url}. Último erro: {last_exc}")


def extract_items_and_pages(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[int], Optional[int]]:
    """
    Esperado no retorno por data (manual):
      {
        "startDate": "2024-05-01",
        "endDate": "",
        "count": "16623",
        "pageNumber": "1 of 333",
        "mergedTickets": [ { ... }, ... ]
      }

    Retorna: (items, page_atual, total_paginas)
    """
    items: List[Dict[str, Any]] = []
    # chave principal segundo manual
    if isinstance(payload.get("mergedTickets"), list):
        items = payload.get("mergedTickets", [])
    # tolerância a variações
    elif isinstance(payload.get("ticketsMerged"), list):
        items = payload.get("ticketsMerged", [])
    elif isinstance(payload.get("TicketsMerged"), list):
        items = payload.get("TicketsMerged", [])

    page_atual = None
    total_paginas = None
    page_number = payload.get("pageNumber") or payload.get("page") or ""
    if isinstance(page_number, str):
        m = re.search(r"(\d+)\s*(?:of|de)\s*(\d+)", page_number, flags=re.IGNORECASE)
        if m:
            page_atual = int(m.group(1))
            total_paginas = int(m.group(2))
    elif isinstance(page_number, dict):
        # fallback caso venha estruturado
        try:
            page_atual = int(page_number.get("current"))
            total_paginas = int(page_number.get("total"))
        except Exception:
            pass

    return items, page_atual, total_paginas


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um item do mergedTickets para colunas do banco.
    Exemplo do manual (item):
      {
        "ticketId": "11337799",
        "mergedTickets": "1",
        "mergedTicketsIds": "11344796;11344797",
        "lastUpdate": "2024-05-23 11:08:13"
      }
    """
    ticket_id_raw = item.get("ticketId") or item.get("id") or item.get("ticketID")
    if ticket_id_raw is None:
        raise ValueError(f"Item sem ticketId: {item}")

    try:
        ticket_id = int(str(ticket_id_raw).strip())
    except Exception as e:
        raise ValueError(f"ticketId inválido: {ticket_id_raw!r}") from e

    merged_tickets_raw = item.get("mergedTickets")
    merged_tickets = None
    if merged_tickets_raw is not None and str(merged_tickets_raw).strip() != "":
        try:
            merged_tickets = int(str(merged_tickets_raw).strip())
        except Exception:
            merged_tickets = None

    merged_ids = item.get("mergedTicketsIds")
    if merged_ids is not None:
        merged_ids = str(merged_ids).strip()

    last_update = parse_dt_last_update(str(item.get("lastUpdate") or ""))

    return {
        "ticket_id": ticket_id,
        "merged_tickets": merged_tickets,
        "merged_tickets_ids": merged_ids,
        "last_update": last_update,
        "raw_json": json.dumps(item, ensure_ascii=False),
    }


def pg_connect() -> psycopg2.extensions.connection:
    host = env("PGHOST")
    port = env("PGPORT", "5432")
    db = env("PGDATABASE")
    user = env("PGUSER")
    pwd = env("PGPASSWORD")

    missing = [k for k, v in [("PGHOST", host), ("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", pwd)] if not v]
    if missing:
        raise RuntimeError(f"Variáveis Postgres ausentes: {', '.join(missing)}")

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=db,
        user=user,
        password=pwd,
        connect_timeout=20,
    )


def ensure_table(cur, schema: str, table: str) -> None:
    cur.execute(f"create schema if not exists {schema}")
    cur.execute(
        f"""
        create table if not exists {schema}.{table} (
            ticket_id           bigint primary key,
            merged_tickets      integer,
            merged_tickets_ids  text,
            last_update         timestamp without time zone,
            raw_json            jsonb,
            updated_at          timestamptz not null default now()
        )
        """
    )
    # tentativa de garantir colunas (caso tabela antiga exista sem elas)
    cur.execute(
        """
        select column_name
          from information_schema.columns
         where table_schema = %s and table_name = %s
        """,
        (schema, table),
    )
    cols = {r[0] for r in cur.fetchall()}

    def add_col(name: str, ddl: str) -> None:
        if name not in cols:
            cur.execute(f"alter table {schema}.{table} add column {ddl}")

    add_col("merged_tickets", "merged_tickets integer")
    add_col("merged_tickets_ids", "merged_tickets_ids text")
    add_col("last_update", "last_update timestamp without time zone")
    add_col("raw_json", "raw_json jsonb")
    add_col("updated_at", "updated_at timestamptz not null default now()")


def get_last_update(cur, schema: str, table: str) -> Optional[datetime]:
    # robusto contra tabelas antigas
    cur.execute(
        """
        select column_name
          from information_schema.columns
         where table_schema = %s and table_name = %s
        """,
        (schema, table),
    )
    cols = {r[0] for r in cur.fetchall()}
    if "last_update" not in cols:
        return None
    cur.execute(f"select max(last_update) from {schema}.{table}")
    return cur.fetchone()[0]


def upsert_items(cur, schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = f"""
        insert into {schema}.{table}
            (ticket_id, merged_tickets, merged_tickets_ids, last_update, raw_json, updated_at)
        values
            (%(ticket_id)s, %(merged_tickets)s, %(merged_tickets_ids)s, %(last_update)s, %(raw_json)s::jsonb, now())
        on conflict (ticket_id) do update set
            merged_tickets      = excluded.merged_tickets,
            merged_tickets_ids  = excluded.merged_tickets_ids,
            last_update         = excluded.last_update,
            raw_json            = excluded.raw_json,
            updated_at          = now()
    """
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    return len(rows)


def daterange_windows(start: date, end: date, window_days: int) -> Iterable[Tuple[date, date]]:
    if window_days < 1:
        window_days = 1
    cur = start
    while cur <= end:
        w_end = min(end, cur + timedelta(days=window_days - 1))
        yield cur, w_end
        cur = w_end + timedelta(days=1)


def sync_window(client: MovideskClient, cur, schema: str, table: str, start_d: date, end_d: date) -> int:
    """
    Sincroniza um intervalo de datas (startDate/endDate) paginando via `page`.
    """
    log(f"tickets_mesclados: /tickets/merged startDate={start_d} endDate={end_d}")
    total_upserts = 0
    page = 1
    total_pages: Optional[int] = None

    while True:
        params = {
            "startDate": start_d.strftime("%Y-%m-%d"),
            "endDate": end_d.strftime("%Y-%m-%d"),
            "page": page,
        }
        r = client.get("/tickets/merged", params=params)

        if r.status_code == 404:
            msg = f"/tickets/merged retornou 404. Verifique se o endpoint está habilitado e se os parâmetros estão no formato yyyy-MM-dd."
            if env("IGNORE_MERGED_404") == "1":
                log(f"[WARN] {msg} IGNORE_MERGED_404=1 -> seguindo sem falhar.")
                return total_upserts
            raise RuntimeError(msg)

        if not r.ok:
            raise RuntimeError(f"Erro HTTP {r.status_code} em /tickets/merged: {r.text[:500]}")

        try:
            payload = r.json()
        except Exception as e:
            raise RuntimeError(f"Resposta não-JSON em /tickets/merged: {r.text[:500]}") from e

        items, page_atual, total_pages_resp = extract_items_and_pages(payload)
        if total_pages_resp is not None:
            total_pages = total_pages_resp

        norm_rows = []
        for it in items:
            try:
                norm_rows.append(normalize_item(it))
            except Exception as e:
                log(f"[WARN] item inválido ignorado: {e}. item={it}")
        if norm_rows:
            total_upserts += upsert_items(cur, schema, table, norm_rows)

        # critério de parada
        if total_pages is not None:
            if page >= total_pages:
                break
        else:
            # se não veio total de páginas, para quando vier vazio
            if not items:
                break

        page += 1

    return total_upserts


def main() -> None:
    token = env("MOVIDESK_TOKEN") or env("MOVI_TOKEN") or env("TOKEN")
    if not token:
        raise RuntimeError("Defina MOVIDESK_TOKEN (ou MOVI_TOKEN).")

    base_url = env("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    schema = env("MOVIDESK_MERGED_SCHEMA", "visualizacao_resolvidos")
    table = env("MOVIDESK_MERGED_TABLE", "tickets_mesclados")
    window_days = int(env("WINDOW_DAYS", "7") or "7")

    today = datetime.now(TZ).date()

    start_env = env("START_DATE")
    end_env = env("END_DATE")

    client = MovideskClient(base_url=base_url, token=token)

    conn = pg_connect()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            ensure_table(cur, schema, table)
            conn.commit()

        with conn.cursor() as cur:
            last_upd = get_last_update(cur, schema, table)
            conn.commit()

        if start_env:
            start_date = parse_date_yyyy_mm_dd(start_env)
        elif last_upd:
            # API por data é yyyy-MM-dd, então usamos a data do last_update e voltamos 1 dia para segurança
            start_date = (last_upd.date() - timedelta(days=1))
        else:
            # default razoável: últimos 7 dias
            start_date = today - timedelta(days=7)

        end_date = parse_date_yyyy_mm_dd(end_env) if end_env else today
        if start_date > end_date:
            log(f"[WARN] start_date ({start_date}) > end_date ({end_date}). Ajustando start_date = end_date.")
            start_date = end_date

        total = 0
        with conn.cursor() as cur:
            for w_start, w_end in daterange_windows(start_date, end_date, window_days):
                try:
                    up = sync_window(client, cur, schema, table, w_start, w_end)
                    total += up
                    conn.commit()
                    log(f"tickets_mesclados: janela {w_start}..{w_end} upserts={up}")
                except Exception as e:
                    conn.rollback()
                    log(f"[ERROR] falha na janela {w_start}..{w_end}: {e}")
                    raise

        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {schema}.{table}")
            count = cur.fetchone()[0]
            conn.commit()

        log(f"tickets_mesclados: sincronização concluída. Upserts nesta execução: {total}. Total na tabela: {count}.")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
