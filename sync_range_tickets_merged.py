#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql


# ----------------------------
# Helpers de ENV
# ----------------------------
def env_str(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v.strip()
    return default


def env_int(*keys: str, default: int) -> int:
    v = env_str(*keys)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def env_bool(*keys: str, default: bool = False) -> bool:
    v = env_str(*keys)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # epoch seconds (fallback)
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # ISO 8601 (ex: 2025-12-24T18:38:50.877Z)
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


# ----------------------------
# Config
# ----------------------------
@dataclass
class Settings:
    # Movidesk
    movidesk_token: str
    base_url: str

    # DB
    db_schema: str
    merged_table: str
    control_table: str

    # Scanner
    limit: int
    rpm: int
    throttle_sec: float
    dry_run: bool


def load_settings() -> Settings:
    # Aceita o padrão do seu repo (MOVIDESK_TOKEN) e mantém compatibilidade com MOVI_TOKEN
    token = env_str("MOVIDESK_TOKEN", "MOVI_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVI_TOKEN) com o token da API Movidesk.")

    base_url = env_str("MOVIDESK_BASE_URL", "MOVI_BASE_URL", default="https://api.movidesk.com/public/v1")

    db_schema = env_str("DB_SCHEMA", default="visualizacao_resolvidos")
    merged_table = env_str("MERGED_TABLE", default="tickets_mesclados")
    control_table = env_str("CONTROL_TABLE", default="range_scan_control")

    # 80 por execução (como você pediu). Compatível com seu padrão RANGE_SCAN_LIMIT.
    limit = env_int("RANGE_SCAN_LIMIT", "LIMIT", default=80)

    # A API tem rate limit baixo; por segurança use 9 rpm (≈ 6.6s entre req).
    rpm = env_int("RANGE_SCAN_RPM", "RPM", default=9)

    # Se você definir throttle explícito (segundos), ele manda.
    throttle_sec = float(env_str("RANGE_SCAN_THROTTLE", default="0") or "0")
    if throttle_sec <= 0:
        throttle_sec = max(60.0 / max(rpm, 1), 0.0)

    dry_run = env_bool("DRY_RUN", default=False)

    return Settings(
        movidesk_token=token,
        base_url=base_url.rstrip("/"),
        db_schema=db_schema,
        merged_table=merged_table,
        control_table=control_table,
        limit=limit,
        rpm=rpm,
        throttle_sec=throttle_sec,
        dry_run=dry_run,
    )


# ----------------------------
# Postgres (igual ao original)
# ----------------------------
def pg_connect():
    # Igual ao seu script que funciona: aceita DATABASE_URL ou NEON_DSN
    dsn = env_str("DATABASE_URL", "NEON_DSN", "POSTGRES_URL", "PG_URL")
    if dsn:
        return psycopg2.connect(dsn)

    host = env_str("PGHOST")
    dbname = env_str("PGDATABASE")
    user = env_str("PGUSER")
    password = env_str("PGPASSWORD")
    port = env_str("PGPORT", default="5432")
    sslmode = env_str("PGSSLMODE")  # opcional

    if not (host and dbname and user and password):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL/NEON_DSN ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    conn_kwargs = dict(host=host, dbname=dbname, user=user, password=password, port=port)
    if sslmode:
        conn_kwargs["sslmode"] = sslmode
    return psycopg2.connect(**conn_kwargs)


# ----------------------------
# DB schema/tables
# ----------------------------
def ensure_schema_and_tables(conn, cfg: Settings) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(cfg.db_schema)))

        # tabela de mesclados (compatível com a do original)
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    ticket_id BIGINT PRIMARY KEY,
                    merged_tickets INTEGER,
                    merged_tickets_ids TEXT,
                    merged_ticket_ids_arr BIGINT[],
                    last_update TIMESTAMPTZ,
                    synced_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(cfg.db_schema), sql.Identifier(cfg.merged_table))
        )

        # control table (mínimo necessário + compatível com outras rotinas)
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    data_inicio TIMESTAMPTZ,
                    data_fim TIMESTAMPTZ,
                    id_inicial BIGINT,
                    id_final BIGINT,
                    id_atual BIGINT,
                    id_atual_merged BIGINT,
                    ultima_data_validada TIMESTAMPTZ
                )
                """
            ).format(sql.Identifier(cfg.db_schema), sql.Identifier(cfg.control_table))
        )

    conn.commit()


def read_open_control_row(conn, cfg: Settings) -> Optional[Dict[str, Any]]:
    """
    Pega a linha "aberta" mais recente (data_fim IS NULL).
    Usa ctid para atualizar exatamente a linha lida.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT ctid, data_inicio, data_fim, id_inicial, id_final,
                       COALESCE(id_atual_merged, id_inicial) AS id_atual_merged
                FROM {}.{}
                WHERE data_fim IS NULL
                ORDER BY data_inicio DESC NULLS LAST
                LIMIT 1
                """
            ).format(sql.Identifier(cfg.db_schema), sql.Identifier(cfg.control_table))
        )
        row = cur.fetchone()
        return dict(row) if row else None


def bootstrap_control_if_needed(conn, cfg: Settings) -> Optional[Dict[str, Any]]:
    """
    Se não existir linha aberta, tenta criar uma usando envs (opcional).
    Se não tiver env, retorna None (scanner encerra OK).
    """
    row = read_open_control_row(conn, cfg)
    if row:
        return row

    id_inicial = env_str("ID_INICIAL_MERGED", "ID_ATUAL_MERGED")
    id_final = env_str("ID_FINAL_MERGED")

    if not (id_inicial and id_final):
        return None

    try:
        id_inicial_i = int(id_inicial)
        id_final_i = int(id_final)
    except ValueError:
        raise RuntimeError("ID_INICIAL_MERGED/ID_ATUAL_MERGED e ID_FINAL_MERGED precisam ser números inteiros.")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {}.{} (data_inicio, id_inicial, id_final, id_atual_merged, ultima_data_validada)
                VALUES (NOW(), %s, %s, %s, NOW())
                """
            ).format(sql.Identifier(cfg.db_schema), sql.Identifier(cfg.control_table)),
            (id_inicial_i, id_final_i, id_inicial_i),
        )
    conn.commit()

    return read_open_control_row(conn, cfg)


def update_control_pointer(conn, cfg: Settings, ctid: str, new_id_atual_merged: int, finished: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                UPDATE {}.{}
                SET id_atual_merged = %s,
                    ultima_data_validada = NOW(),
                    data_fim = CASE WHEN %s THEN NOW() ELSE data_fim END
                WHERE ctid = %s
                """
            ).format(sql.Identifier(cfg.db_schema), sql.Identifier(cfg.control_table)),
            (new_id_atual_merged, finished, ctid),
        )
    conn.commit()


# ----------------------------
# Movidesk API
# ----------------------------
class MovideskClient:
    def __init__(self, base_url: str, token: str, throttle_sec: float, timeout_sec: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.throttle_sec = max(throttle_sec, 0.0)
        self.timeout_sec = timeout_sec
        self._last_call = 0.0
        self.session = requests.Session()

    def _throttle(self):
        if self.throttle_sec <= 0:
            return
        now = time.time()
        dt = now - self._last_call
        if dt < self.throttle_sec:
            time.sleep(self.throttle_sec - dt)
        self._last_call = time.time()

    def get_ticket_merged(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        """
        Tenta validar por ID.
        Algumas documentações usam 'id', outras 'ticketId' — então tentamos os dois.
        """
        self._throttle()

        # 1) tenta id=
        url = f"{self.base_url}/tickets/merged"
        params = {"token": self.token, "id": ticket_id}

        r = self.session.get(url, params=params, timeout=self.timeout_sec)

        # 404 = não mesclado / não encontrado => não é erro
        if r.status_code == 404:
            return None

        # se der 400, tenta ticketId=
        if r.status_code == 400:
            self._throttle()
            params2 = {"token": self.token, "ticketId": ticket_id}
            r = self.session.get(url, params=params2, timeout=self.timeout_sec)
            if r.status_code == 404:
                return None

        # rate limit: backoff simples e tenta 1 vez
        if r.status_code == 429:
            time.sleep(15)
            self._throttle()
            r = self.session.get(url, params=params, timeout=self.timeout_sec)
            if r.status_code == 404:
                return None

        r.raise_for_status()

        data = r.json()
        # Pode vir lista ou objeto
        if isinstance(data, list):
            if not data:
                return None
            # Para consulta por ticket, normalmente é 1 item
            return data[0]
        if isinstance(data, dict):
            return data
        return None


# ----------------------------
# Upsert
# ----------------------------
def normalize_merged_payload(ticket_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normaliza o retorno para o formato da tabela tickets_mesclados.
    Espera campos comuns:
      - ticketId
      - mergedTickets
      - mergedTicketsIds (string "123;456;789")
      - lastUpdate
    """
    tid = payload.get("ticketId") or payload.get("id") or ticket_id

    merged_ids_txt = payload.get("mergedTicketsIds") or payload.get("mergedTicketIds") or payload.get("mergedTicketsId")
    if merged_ids_txt is None:
        # algumas respostas podem vir com lista; tenta derivar
        merged_list = payload.get("mergedTickets") if isinstance(payload.get("mergedTickets"), list) else None
        if merged_list:
            merged_ids_txt = ";".join(str(x) for x in merged_list)

    if not merged_ids_txt:
        return None

    merged_ids_txt = str(merged_ids_txt).strip()
    parts = [p.strip() for p in merged_ids_txt.split(";") if p.strip()]

    merged_arr: List[int] = []
    for p in parts:
        try:
            merged_arr.append(int(p))
        except ValueError:
            continue

    if not merged_arr:
        return None

    merged_tickets = payload.get("mergedTickets")
    if not isinstance(merged_tickets, int):
        merged_tickets = len(merged_arr)

    last_update = parse_dt(payload.get("lastUpdate") or payload.get("last_update") or payload.get("updatedAt"))

    return {
        "ticket_id": int(tid),
        "merged_tickets": int(merged_tickets),
        "merged_tickets_ids": merged_ids_txt,
        "merged_ticket_ids_arr": merged_arr,
        "last_update": last_update,
        "synced_at": utcnow(),
    }


def upsert_rows(conn, cfg: Settings, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    cols = ["ticket_id", "merged_tickets", "merged_tickets_ids", "merged_ticket_ids_arr", "last_update", "synced_at"]
    values = [
        (
            r["ticket_id"],
            r["merged_tickets"],
            r["merged_tickets_ids"],
            r["merged_ticket_ids_arr"],
            r["last_update"],
            r["synced_at"],
        )
        for r in rows
    ]

    query = sql.SQL(
        """
        INSERT INTO {}.{} ({})
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            merged_ticket_ids_arr = EXCLUDED.merged_ticket_ids_arr,
            last_update = EXCLUDED.last_update,
            synced_at = EXCLUDED.synced_at
        """
    ).format(
        sql.Identifier(cfg.db_schema),
        sql.Identifier(cfg.merged_table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
    )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, values, page_size=1000)

    conn.commit()
    return len(rows)


# ----------------------------
# Main
# ----------------------------
def setup_logger() -> logging.Logger:
    level = env_str("LOG_LEVEL", default="INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | range_scan | %(message)s",
    )
    return logging.getLogger("range_scan")


def main():
    log = setup_logger()
    cfg = load_settings()

    log.info(
        "scanner iniciando | schema=%s tabela=%s control=%s | limit=%s | rpm=%s | throttle=%.2fs | dry_run=%s",
        cfg.db_schema, cfg.merged_table, cfg.control_table, cfg.limit, cfg.rpm, cfg.throttle_sec, cfg.dry_run
    )

    conn = pg_connect()
    ensure_schema_and_tables(conn, cfg)

    control = bootstrap_control_if_needed(conn, cfg)
    if not control:
        log.info("nenhuma linha aberta em %s.%s e sem envs de bootstrap -> encerrando OK", cfg.db_schema, cfg.control_table)
        return

    ctid = control["ctid"]
    id_final = control.get("id_final")
    id_atual = control.get("id_atual_merged")

    if id_final is None or id_atual is None:
        raise RuntimeError("range_scan_control precisa ter id_final e id_atual_merged (ou id_inicial).")

    id_final = int(id_final)
    id_atual = int(id_atual)

    if id_atual < id_final:
        log.info("scanner já concluído (id_atual_merged=%s < id_final=%s). Fechando linha.", id_atual, id_final)
        if not cfg.dry_run:
            update_control_pointer(conn, cfg, ctid, id_atual, finished=True)
        return

    low = max(id_final, id_atual - cfg.limit + 1)
    ids = list(range(id_atual, low - 1, -1))

    client = MovideskClient(cfg.base_url, cfg.movidesk_token, cfg.throttle_sec, timeout_sec=30.0)

    to_upsert: List[Dict[str, Any]] = []
    checked = 0
    merged_found = 0

    for tid in ids:
        checked += 1
        try:
            payload = client.get_ticket_merged(tid)
        except requests.HTTPError as e:
            log.warning("ticket_id=%s | http_error=%s", tid, str(e))
            continue
        except Exception as e:
            log.warning("ticket_id=%s | error=%s", tid, str(e))
            continue

        if not payload:
            continue

        norm = normalize_merged_payload(tid, payload)
        if not norm:
            continue

        merged_found += 1
        to_upsert.append(norm)

    upserted = 0
    if cfg.dry_run:
        log.info("dry_run=True | checked=%s | merged_found=%s | upsert_skip=%s", checked, merged_found, len(to_upsert))
    else:
        upserted = upsert_rows(conn, cfg, to_upsert)

    # Move o ponteiro para baixo
    next_id = low - 1
    finished = next_id < id_final

    if finished:
        # marca como fechado e deixa id_atual_merged como id_final (sinal de fim)
        new_ptr = id_final
        log.info("scanner concluiu a faixa | checked=%s | merged_found=%s | upserted=%s | fim=%s", checked, merged_found, upserted, id_final)
    else:
        new_ptr = next_id
        log.info("scanner ok | checked=%s | merged_found=%s | upserted=%s | novo_id_atual_merged=%s", checked, merged_found, upserted, new_ptr)

    if not cfg.dry_run:
        update_control_pointer(conn, cfg, ctid, new_ptr, finished=finished)


if __name__ == "__main__":
    main()
