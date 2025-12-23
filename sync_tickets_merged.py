#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras

try:
    # pip install python-dateutil
    from dateutil import parser as dtparser
except Exception as e:
    dtparser = None


LOG = logging.getLogger("tickets_mesclados")


# ----------------------------
# Config
# ----------------------------
@dataclass
class Config:
    # Movidesk
    base_url: str
    token: str

    # DB
    database_url: str
    db_schema: str
    table_name: str

    # Sync window
    window_days: int
    lookback_days_if_empty: int

    # Rate limit (manual fala 10 req/min)
    min_seconds_between_requests: float

    # HTTP
    timeout_seconds: int
    max_retries: int

    # Optional overrides
    start_date_override: Optional[str]
    end_date_override: Optional[str]
    ticket_id_override: Optional[str]


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"Env {name} inválida (esperado int): {v}")


def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        raise RuntimeError(f"Env {name} inválida (esperado float): {v}")


def load_config() -> Config:
    token = (os.getenv("MOVIDESK_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (secret).")

    base_url = (os.getenv("MOVIDESK_BASE_URL") or "https://api.movidesk.com/public/v1").rstrip("/")

    # Aceita DATABASE_URL ou NEON_DSN (seu log mostrou NEON_DSN)
    database_url = (os.getenv("DATABASE_URL") or os.getenv("NEON_DSN") or "").strip()
    if not database_url:
        # fallback components
        pghost = os.getenv("PGHOST")
        pgdb = os.getenv("PGDATABASE")
        pguser = os.getenv("PGUSER")
        pgpass = os.getenv("PGPASSWORD")
        pgport = os.getenv("PGPORT") or "5432"
        if pghost and pgdb and pguser and pgpass:
            database_url = f"postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}"
        else:
            raise RuntimeError(
                "Faltam variáveis de Postgres. Use DATABASE_URL (ou NEON_DSN) "
                "ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
            )

    cfg = Config(
        base_url=base_url,
        token=token,
        database_url=database_url,
        db_schema=os.getenv("DB_SCHEMA") or "visualizacao_resolvidos",
        table_name=os.getenv("DB_TABLE") or "tickets_mesclados",
        window_days=getenv_int("WINDOW_DAYS", 7),
        lookback_days_if_empty=getenv_int("LOOKBACK_DAYS", 30),
        min_seconds_between_requests=getenv_float("MIN_SECONDS_BETWEEN_REQUESTS", 6.5),
        timeout_seconds=getenv_int("HTTP_TIMEOUT_SECONDS", 60),
        max_retries=getenv_int("HTTP_MAX_RETRIES", 6),
        start_date_override=(os.getenv("START_DATE") or "").strip() or None,
        end_date_override=(os.getenv("END_DATE") or "").strip() or None,
        ticket_id_override=(os.getenv("TICKET_ID") or "").strip() or None,
    )
    return cfg


# ----------------------------
# Utils: dates / parsing
# ----------------------------
def parse_date_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def floor_to_day_utc(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=timezone.utc)


def ceil_to_day_end_utc(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, dt.day, 23, 59, 59, tzinfo=timezone.utc)


def to_utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_movidesk_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # manual costuma retornar ISO; vamos suportar ISO e variações
    try:
        if dtparser is not None:
            dt = dtparser.isoparse(s)
        else:
            # fallback simples
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # se vier algo muito diferente, não quebra o sync
        return None


def parse_ids(value: Any) -> List[int]:
    """
    mergedTicketsIds no manual aparece como string (às vezes com múltiplos ids).
    Também suportamos lista.
    """
    if value is None:
        return []
    if isinstance(value, list):
        out: List[int] = []
        for x in value:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    if isinstance(value, (int, float)):
        try:
            return [int(value)]
        except Exception:
            return []
    s = str(value).strip()
    if not s:
        return []
    # extrai qualquer sequência de dígitos
    return [int(x) for x in re.findall(r"\d+", s)]


# ----------------------------
# DB
# ----------------------------
def pg_connect(database_url: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    return conn


def ensure_schema_and_table(conn, schema: str, table: str) -> None:
    """
    Cria schema/tabela e adiciona colunas faltantes (resolve seu erro merged_ticket_id não existe).
    """
    ddl_create = f"""
    CREATE SCHEMA IF NOT EXISTS "{schema}";
    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
        ticket_id BIGINT NOT NULL,
        merged_ticket_id BIGINT NOT NULL,
        merged_tickets INTEGER NULL,
        merged_tickets_ids TEXT NULL,
        last_update TIMESTAMP NULL,
        fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
        raw JSONB NULL,
        PRIMARY KEY (ticket_id, merged_ticket_id)
    );
    """

    required_cols = {
        "ticket_id": 'BIGINT',
        "merged_ticket_id": 'BIGINT',
        "merged_tickets": 'INTEGER',
        "merged_tickets_ids": 'TEXT',
        "last_update": 'TIMESTAMP',
        "fetched_at": 'TIMESTAMP',
        "raw": 'JSONB',
    }

    with conn.cursor() as cur:
        cur.execute(ddl_create)

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        existing = {r[0] for r in cur.fetchall()}

        for col, coltype in required_cols.items():
            if col not in existing:
                LOG.warning('Coluna ausente "%s". Fazendo ALTER TABLE para adicionar...', col)
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col}" {coltype} NULL;')

        # Índices úteis
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_last_update ON "{schema}"."{table}" (last_update);'
        )
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{table}_ticket_id ON "{schema}"."{table}" (ticket_id);'
        )

    conn.commit()


def get_last_success_dt(conn, schema: str, table: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(f'SELECT MAX(last_update) FROM "{schema}"."{table}";')
        v = cur.fetchone()[0]
        if v is None:
            return None
        # psycopg2 já retorna datetime
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        return None


def upsert_rows(
    conn,
    schema: str,
    table: str,
    rows: List[Dict[str, Any]],
) -> int:
    if not rows:
        return 0

    sql = f"""
    INSERT INTO "{schema}"."{table}"
        (ticket_id, merged_ticket_id, merged_tickets, merged_tickets_ids, last_update, fetched_at, raw)
    VALUES %s
    ON CONFLICT (ticket_id, merged_ticket_id) DO UPDATE SET
        merged_tickets = EXCLUDED.merged_tickets,
        merged_tickets_ids = EXCLUDED.merged_tickets_ids,
        last_update = EXCLUDED.last_update,
        fetched_at = EXCLUDED.fetched_at,
        raw = EXCLUDED.raw
    ;
    """

    values = []
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)  # armazenar sem tz
    for r in rows:
        last_update = r.get("last_update")
        if isinstance(last_update, datetime):
            last_update = last_update.astimezone(timezone.utc).replace(tzinfo=None)

        values.append(
            (
                int(r["ticket_id"]),
                int(r["merged_ticket_id"]),
                r.get("merged_tickets"),
                r.get("merged_tickets_ids"),
                last_update,
                fetched_at,
                psycopg2.extras.Json(r.get("raw") or {}, dumps=json.dumps),
            )
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    return len(values)


# ----------------------------
# Movidesk HTTP
# ----------------------------
class RateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval = max(0.0, float(min_interval_seconds))
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.time()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()


def movidesk_get_json(
    session: requests.Session,
    limiter: RateLimiter,
    url: str,
    params: Dict[str, Any],
    timeout_seconds: int,
    max_retries: int,
) -> Any:
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            limiter.wait()
            resp = session.get(url, params=params, timeout=timeout_seconds)

            # Se o Movidesk aplicar bloqueio progressivo, pode vir 429/403/400 dependendo do cenário
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 2 ** attempt)
                LOG.warning("HTTP %s em %s (tentativa %s/%s). Aguardando %ss...",
                            resp.status_code, resp.url, attempt, max_retries, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                # deixa explícito no log e sobe erro
                raise RuntimeError(f"Endpoint retornou 404: {resp.url}")

            resp.raise_for_status()

            try:
                return resp.json()
            except Exception as e:
                raise RuntimeError(f"Falha ao decodificar JSON: {e} | body={resp.text[:500]}")

        except Exception as e:
            last_err = e
            wait = min(60, 2 ** attempt)
            if attempt < max_retries:
                LOG.warning("Erro HTTP: %s (tentativa %s/%s). Aguardando %ss...", e, attempt, max_retries, wait)
                time.sleep(wait)
            else:
                break

    raise RuntimeError(f"Falha após {max_retries} tentativas. Último erro: {last_err}")


def fetch_tickets_merged_window(
    cfg: Config,
    session: requests.Session,
    limiter: RateLimiter,
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, Any]]:
    """
    Chama GET /tickets/merged paginado.
    Manual: token obrigatório + startDate/endDate (UTC) + page.
    """
    endpoint = f"{cfg.base_url}/tickets/merged"
    page = 1
    all_items: List[Dict[str, Any]] = []

    while True:
        params: Dict[str, Any] = {"token": cfg.token, "page": page}

        if cfg.ticket_id_override:
            # manual cita parâmetro "id"
            params["id"] = cfg.ticket_id_override
        else:
            params["startDate"] = to_utc_iso(start_dt)
            params["endDate"] = to_utc_iso(end_dt)

        LOG.info("buscando /tickets/merged | page=%s | start=%s end=%s",
                 page, params.get("startDate"), params.get("endDate"))

        data = movidesk_get_json(
            session=session,
            limiter=limiter,
            url=endpoint,
            params=params,
            timeout_seconds=cfg.timeout_seconds,
            max_retries=cfg.max_retries,
        )

        if not data:
            break

        if not isinstance(data, list):
            # algumas APIs retornam { "items": [...] }
            if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                data = data["items"]
            else:
                LOG.warning("Resposta inesperada (não-lista). Tipo=%s. Conteúdo parcial=%s", type(data), str(data)[:200])
                break

        all_items.extend(data)

        # paginação: se veio menos que 100, provavelmente acabou
        if len(data) < 100:
            break

        page += 1

        # trava de segurança
        if page > 10000:
            LOG.warning("page>10000, abortando paginação por segurança.")
            break

    return all_items


def normalize_items_to_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Converte cada item do /tickets/merged em linhas (ticket_id, merged_ticket_id).
    """
    rows: List[Dict[str, Any]] = []
    for it in items:
        # pelo manual: ticketId, mergedTickets, mergedTicketsIds, lastUpdate
        ticket_id = it.get("ticketId") or it.get("id") or it.get("ticketID") or it.get("TicketId")
        if ticket_id is None:
            continue

        merged_tickets = it.get("mergedTickets")
        merged_ids_raw = it.get("mergedTicketsIds")

        merged_ids = parse_ids(merged_ids_raw)
        last_update = parse_movidesk_datetime(it.get("lastUpdate"))

        # Se vier mergedTickets > 0 mas sem ids, loga e pula (não temos como criar PK)
        if (merged_tickets or 0) > 0 and not merged_ids:
            LOG.warning("Ticket %s tem mergedTickets=%s mas mergedTicketsIds vazio. Item=%s",
                        ticket_id, merged_tickets, str(it)[:250])
            continue

        for mid in merged_ids:
            rows.append(
                {
                    "ticket_id": int(ticket_id),
                    "merged_ticket_id": int(mid),
                    "merged_tickets": int(merged_tickets) if merged_tickets is not None else None,
                    "merged_tickets_ids": str(merged_ids_raw) if merged_ids_raw is not None else None,
                    "last_update": last_update,
                    "raw": it,
                }
            )
    return rows


def iter_date_windows(start_dt: datetime, end_dt: datetime, window_days: int) -> Iterable[Tuple[datetime, datetime]]:
    """
    Gera janelas [start..end] em UTC, com cortes de window_days (ex.: 7 dias).
    """
    if window_days <= 0:
        window_days = 7

    cur = start_dt
    while cur <= end_dt:
        w_start = cur
        w_end = min(end_dt, cur + timedelta(days=window_days - 1))
        yield w_start, w_end
        cur = w_end + timedelta(days=1)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = load_config()

    LOG.info(
        "sync iniciando | schema=%s tabela=%s | window_days=%s | lookback=%s | base_url=%s",
        cfg.db_schema,
        cfg.table_name,
        cfg.window_days,
        cfg.lookback_days_if_empty,
        cfg.base_url,
    )

    conn = pg_connect(cfg.database_url)
    try:
        ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

        # Determina intervalo
        now_utc = datetime.now(timezone.utc)

        if cfg.start_date_override and cfg.end_date_override:
            start_d = parse_date_yyyy_mm_dd(cfg.start_date_override)
            end_d = parse_date_yyyy_mm_dd(cfg.end_date_override)
            start_dt = datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc)
            end_dt = datetime(end_d.year, end_d.month, end_d.day, tzinfo=timezone.utc)
        else:
            since = get_last_success_dt(conn, cfg.db_schema, cfg.table_name)
            if since is None:
                start_dt = now_utc - timedelta(days=cfg.lookback_days_if_empty)
            else:
                # buffer pequeno pra não perder eventos
                start_dt = since - timedelta(minutes=10)
            end_dt = now_utc

        start_dt = floor_to_day_utc(start_dt)
        end_dt = ceil_to_day_end_utc(end_dt)

        LOG.info("janela UTC: %s .. %s", to_utc_iso(start_dt), to_utc_iso(end_dt))

        session = requests.Session()
        limiter = RateLimiter(cfg.min_seconds_between_requests)

        total_rows = 0
        total_items = 0

        for w_start, w_end in iter_date_windows(start_dt, end_dt, cfg.window_days):
            w_start = floor_to_day_utc(w_start)
            w_end = ceil_to_day_end_utc(w_end)

            items = fetch_tickets_merged_window(cfg, session, limiter, w_start, w_end)
            total_items += len(items)

            rows = normalize_items_to_rows(items)

            # upsert em lotes
            batch_size = 2000
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                try:
                    total_rows += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)
                except Exception:
                    conn.rollback()
                    raise

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{cfg.db_schema}"."{cfg.table_name}";')
            total_in_table = cur.fetchone()[0]

        LOG.info(
            "sincronização concluída | itens_api=%s | linhas_upsert=%s | total_tabela=%s",
            total_items,
            total_rows,
            total_in_table,
        )

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.exception("Falha no sync: %s", e)
        sys.exit(1)
