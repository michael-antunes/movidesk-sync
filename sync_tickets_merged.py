#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import math
import traceback
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


# =========================
# Config / Utils
# =========================

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} | {msg}", flush=True)


def warn(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} | [WARN] {msg}", flush=True)


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default


def parse_dt_any(s: Optional[str]) -> Optional[datetime]:
    """
    Tenta parsear timestamps comuns (ISO 8601, "YYYY-MM-DD HH:MM:SS", etc).
    Retorna datetime timezone-aware (UTC) quando possível.
    """
    if not s:
        return None
    ss = s.strip()
    if not ss:
        return None

    # Normaliza Z
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"

    # Tenta ISO
    try:
        dt = datetime.fromisoformat(ss)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass

    # Tenta "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(ss, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def to_yyyy_mm_dd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def chunked(it: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(it), n):
        yield it[i:i+n]


# =========================
# Postgres
# =========================

def pg_connect():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # DATABASE_URL pode vir com sslmode=require etc.
        return psycopg2.connect(db_url)

    host = os.getenv("PGHOST")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pwd = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")

    if not (host and dbname and user and pwd):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(
        host=host, port=int(port), dbname=dbname, user=user, password=pwd
    )


def ensure_schema_and_table(conn, schema: str, table: str) -> None:
    """
    Cria schema/tabela e garante colunas essenciais.
    Estrutura: 1 linha por par (ticket_id, merged_ticket_id).
    """
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
            ticket_id BIGINT NOT NULL,
            merged_ticket_id BIGINT NOT NULL,
            merged_tickets INTEGER NULL,
            merged_tickets_ids TEXT NULL,
            last_update TIMESTAMPTZ NULL,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ticket_id, merged_ticket_id)
        );
        """)

        # Garante colunas caso a tabela já existisse diferente
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS merged_ticket_id BIGINT;')
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS merged_tickets INTEGER;')
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS merged_tickets_ids TEXT;')
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS last_update TIMESTAMPTZ;')
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ NOT NULL DEFAULT now();')

    conn.commit()


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple]) -> int:
    """
    rows: (ticket_id, merged_ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at)
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO "{schema}"."{table}"
        (ticket_id, merged_ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at)
    VALUES %s
    ON CONFLICT (ticket_id, merged_ticket_id)
    DO UPDATE SET
        merged_tickets = EXCLUDED.merged_tickets,
        merged_tickets_ids = EXCLUDED.merged_tickets_ids,
        last_update = EXCLUDED.last_update,
        fetched_at = EXCLUDED.fetched_at
    ;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def count_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        return int(cur.fetchone()[0])


# =========================
# Movidesk API
# =========================

@dataclass
class MoviCfg:
    base_url: str
    token: str
    schema: str
    table: str
    lookback_days: int
    window_days: int
    sleep_s: float


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 60) -> Any:
    """
    GET com retry simples em 429/5xx.
    """
    session = requests.Session()
    backoff = 1.5
    max_tries = 6

    # Não printar token
    safe_params = dict(params)
    if "token" in safe_params:
        safe_params["token"] = "***"

    for attempt in range(1, max_tries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                warn(f"HTTP {r.status_code} em {url} params={safe_params} | tentativa {attempt}/{max_tries}")
                time.sleep(backoff ** attempt)
                continue

            # 404 pode acontecer no tipo 1 (id não encontrado) — vamos tratar acima na lógica.
            r.raise_for_status()

            # Movidesk geralmente responde JSON
            return r.json()

        except requests.HTTPError as e:
            # Erros 4xx que não são retry
            raise
        except Exception as e:
            warn(f"Erro inesperado HTTP em {url} params={safe_params} | tentativa {attempt}/{max_tries}: {e}")
            time.sleep(backoff ** attempt)

    raise RuntimeError(f"Falha após {max_tries} tentativas: {url}")


def normalize_merged_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Aceita:
    - TIPO 2: dict com chave mergedTickets (lista)
    - TIPO 1: dict de um item (ticketId/mergedTicketsIds/lastUpdate)
    - lista direta de itens
    - (opcional) dict com "value" (estilo OData) se ocorrer
    """
    if payload is None:
        return []

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        # Tipo 2 (manual): wrapper com mergedTickets
        mt = payload.get("mergedTickets")
        if isinstance(mt, list):
            return [x for x in mt if isinstance(x, dict)]

        # OData-style
        val = payload.get("value")
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]

        # Tipo 1: um único item
        if any(k in payload for k in ("ticketId", "id", "mergedTicketsIds")):
            return [payload]

        # Não reconhecido
        return []

    # Não reconhecido
    return []


def explode_rows(items: List[Dict[str, Any]]) -> List[Tuple]:
    """
    Converte itens do /tickets/merged em linhas:
    (ticket_id, merged_ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at)
    """
    out: List[Tuple] = []
    fetched_at = datetime.now(timezone.utc)

    for it in items:
        # Alguns ambientes podem retornar "ticketId" (manual) ou já vir como "id" (custom)
        tid_raw = it.get("ticketId", it.get("id"))
        if tid_raw is None:
            continue

        try:
            ticket_id = int(str(tid_raw).strip())
        except Exception:
            continue

        merged_tickets_raw = it.get("mergedTickets")
        merged_tickets = None
        if merged_tickets_raw is not None and str(merged_tickets_raw).strip() != "":
            try:
                merged_tickets = int(str(merged_tickets_raw).strip())
            except Exception:
                merged_tickets = None

        merged_ids_str = it.get("mergedTicketsIds")
        merged_ids_str = "" if merged_ids_str is None else str(merged_ids_str).strip()

        last_update = parse_dt_any(it.get("lastUpdate"))
        # Se não parseou, tenta guardar nulo mesmo.

        # mergedTicketsIds pode ser "123;456"
        merged_ids: List[int] = []
        if merged_ids_str:
            parts = [p.strip() for p in merged_ids_str.split(";") if p.strip()]
            for p in parts:
                try:
                    merged_ids.append(int(p))
                except Exception:
                    pass

        # Se não vierem IDs (estranho), ainda assim registra 1 linha com merged_ticket_id = 0
        if not merged_ids:
            out.append((ticket_id, 0, merged_tickets, merged_ids_str or None, last_update, fetched_at))
            continue

        for mid in merged_ids:
            out.append((ticket_id, mid, merged_tickets, merged_ids_str, last_update, fetched_at))

    return out


def fetch_merged_window(cfg: MoviCfg, start_d: date, end_d: date) -> List[Dict[str, Any]]:
    """
    Busca por intervalo (Tipo 2), paginando por 'page'.
    """
    url = cfg.base_url.rstrip("/") + "/tickets/merged"
    items_all: List[Dict[str, Any]] = []

    page = 1
    while True:
        params = {
            "token": cfg.token,
            "startDate": to_yyyy_mm_dd(start_d),
            "endDate": to_yyyy_mm_dd(end_d),
            "page": page,
        }

        payload = None
        try:
            payload = http_get_json(url, params=params, timeout=90)
        except requests.HTTPError as e:
            # Se vier 404 aqui, pode ser endpoint indisponível no seu tenant
            # ou (menos provável) comportamento estranho. Vamos logar e abortar a janela.
            warn(f"/tickets/merged retornou HTTP {getattr(e.response,'status_code',None)} para startDate={params['startDate']} endDate={params['endDate']}.")
            break

        items = normalize_merged_payload(payload)

        # Se veio dict e não reconheceu formato, loga um exemplo
        if isinstance(payload, dict) and not items:
            exemplo = json.dumps(payload, ensure_ascii=False)[:300]
            warn(f"payload dict não reconhecido em /tickets/merged | exemplo={exemplo}")

        if not items:
            break

        items_all.extend(items)

        # Se for tipo 2 wrapper, pode vir 'count' e 'pageNumber'
        # Mesmo assim, como não há pageSize no manual, paramos quando vier vazio na próxima página.
        page += 1
        time.sleep(cfg.sleep_s)

        # proteção contra loop infinito
        if page > 5000:
            warn("Abortando paginação: page > 5000 (proteção).")
            break

    return items_all


# =========================
# Main
# =========================

def main() -> None:
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVI_TOKEN") or ""
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVI_TOKEN).")

    cfg = MoviCfg(
        base_url=getenv_str("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1"),
        token=token,
        schema=getenv_str("DB_SCHEMA", "visualizacao_resolvidos"),
        table=getenv_str("DB_TABLE", "tickets_mesclados"),
        lookback_days=getenv_int("LOOKBACK_DAYS", 30),
        window_days=getenv_int("WINDOW_DAYS", 7),
        sleep_s=float(getenv_str("REQUEST_SLEEP_S", "0.4") or "0.4"),
    )

    # Intervalo
    today_utc = datetime.now(timezone.utc).date()
    start_base = today_utc - timedelta(days=cfg.lookback_days)
    end_base = today_utc

    log(f"tickets_mesclados: sync iniciando | schema={cfg.schema} tabela={cfg.table} | janela={start_base}..{end_base} (window_days={cfg.window_days})")

    conn = pg_connect()
    try:
        ensure_schema_and_table(conn, cfg.schema, cfg.table)

        total_api_items = 0
        total_upserts = 0

        cur_start = start_base
        while cur_start <= end_base:
            cur_end = min(cur_start + timedelta(days=cfg.window_days - 1), end_base)
            log(f"tickets_mesclados: buscando /tickets/merged startDate={cur_start} endDate={cur_end}")

            items = fetch_merged_window(cfg, cur_start, cur_end)
            total_api_items += len(items)

            rows = explode_rows(items)
            if rows:
                up = upsert_rows(conn, cfg.schema, cfg.table, rows)
                total_upserts += up
                log(f"tickets_mesclados: upsert ok | itens_api={len(items)} rows={len(rows)} upserts={up}")
            else:
                log(f"tickets_mesclados: nada a inserir | itens_api={len(items)}")

            cur_start = cur_end + timedelta(days=1)

        total_tbl = count_rows(conn, cfg.schema, cfg.table)
        log(f"tickets_mesclados: sync finalizada | itens_api={total_api_items} | lines_upsert={total_upserts} | total_tabela={total_tbl}")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        warn("Falha no sync_tickets_merged.py")
        warn(str(e))
        traceback.print_exc()
        sys.exit(1)
