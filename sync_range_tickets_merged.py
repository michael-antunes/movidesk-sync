#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Range scanner de Tickets_Merged (Movidesk) por ID.

Como funciona:
- Existe uma tabela de controle (default: visualizacao_resolvidos.range_scan_control)
  com uma "linha aberta" (data_fim IS NULL) contendo id_inicial, id_final e id_atual_merged.
- A cada execução o script valida N IDs (LIMIT) em ordem decrescente começando em id_atual_merged.
- Para cada ticket_id, consulta GET /tickets/merged?id=<ticket_id>.
  * Se houver resposta com mergedTickets > 0, faz upsert em tickets_mesclados.
  * Se não existir/404 ou mergedTickets == 0, ignora.
- Ao final atualiza id_atual_merged no controle. Quando chega no id_final, fecha a linha (data_fim = now()).

ENV (compatível com seu workflow original):
  Movidesk:
    - MOVIDESK_TOKEN (ou MOVI_TOKEN) [obrigatório]
    - MOVIDESK_BASE_URL (default: https://api.movidesk.com/public/v1)

  Postgres:
    - NEON_DSN (ou DATABASE_URL / POSTGRES_URL / PG_URL)
      OU PGHOST/PGDATABASE/PGUSER/PGPASSWORD (opcional PGPORT)

  Controle:
    - DB_SCHEMA (default visualizacao_resolvidos)
    - TABLE_NAME (default tickets_mesclados)
    - CONTROL_TABLE (default range_scan_control)

  Execução:
    - LIMIT (default 80)
    - RPM (default 9)  # requests/minuto
    - DRY_RUN (default false)

  Bootstrap (se não houver linha aberta no controle):
    - ID_ATUAL_MERGED e ID_FINAL_MERGED (opcionais)
"""

from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql


LOG = logging.getLogger("range_scan")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v


def _bool_env(name: str, default: bool = False) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        raise RuntimeError(f"Env {name} inválida: esperado inteiro, veio {v!r}")


def _float_env(name: str, default: float) -> float:
    v = _env(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        raise RuntimeError(f"Env {name} inválida: esperado float, veio {v!r}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def parse_merged_ids(ids_str: Optional[str]) -> Optional[List[int]]:
    if not ids_str:
        return None
    parts = [p.strip() for p in ids_str.split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            continue
    return out or None


@dataclass(frozen=True)
class Settings:
    movi_token: str
    base_url: str
    dsn: str
    db_schema: str
    table_name: str
    control_table: str
    limit: int
    rpm: float
    dry_run: bool
    timeout_s: int = 30


def load_settings() -> Settings:
    token = _env("MOVIDESK_TOKEN") or _env("MOVI_TOKEN") or _env("MOVIDESK_API_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (ou MOVI_TOKEN).")

    base_url = _env("MOVIDESK_BASE_URL", "https://api.movidesk.com/public/v1").rstrip("/")

    dsn = _env("NEON_DSN") or _env("DATABASE_URL") or _env("POSTGRES_URL") or _env("PG_URL")
    if not dsn:
        host = _env("PGHOST")
        db = _env("PGDATABASE")
        user = _env("PGUSER")
        pwd = _env("PGPASSWORD")
        port = _env("PGPORT", "5432")
        if not all([host, db, user, pwd]):
            raise RuntimeError(
                "Faltam variáveis de Postgres. Use NEON_DSN/DATABASE_URL (recomendado) "
                "ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
            )
        dsn = f"host={host} dbname={db} user={user} password={pwd} port={port}"

    schema = _env("DB_SCHEMA", "visualizacao_resolvidos")
    table = _env("TABLE_NAME", "tickets_mesclados")
    control = _env("CONTROL_TABLE", "range_scan_control")

    limit = _int_env("LIMIT", 80)
    rpm = _float_env("RPM", 9.0)
    if rpm <= 0:
        raise RuntimeError("RPM deve ser > 0")
    dry_run = _bool_env("DRY_RUN", False)

    return Settings(
        movi_token=token,
        base_url=base_url,
        dsn=dsn,
        db_schema=schema,
        table_name=table,
        control_table=control,
        limit=limit,
        rpm=rpm,
        dry_run=dry_run,
    )


def pg_connect(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def ensure_tables(conn, schema: str, table: str, control: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

        # destino
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    ticket_id             BIGINT PRIMARY KEY,
                    merged_tickets        INTEGER,
                    merged_tickets_ids    TEXT,
                    merged_ticket_ids_arr BIGINT[],
                    last_update           TIMESTAMPTZ,
                    synced_at             TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )

        # garante colunas (caso tabela antiga)
        for col, typ in [
            ("merged_tickets", "INTEGER"),
            ("merged_tickets_ids", "TEXT"),
            ("merged_ticket_ids_arr", "BIGINT[]"),
            ("last_update", "TIMESTAMPTZ"),
            ("synced_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
        ]:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} " + typ).format(
                    sql.Identifier(schema), sql.Identifier(table), sql.Identifier(col)
                )
            )

        # controle
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    data_inicio           TIMESTAMPTZ NOT NULL DEFAULT now(),
                    data_fim              TIMESTAMPTZ,
                    ultima_data_validada  TIMESTAMPTZ,
                    id_inicial            BIGINT,
                    id_final              BIGINT,
                    id_atual              BIGINT,
                    id_atual_merged       BIGINT
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(control))
        )

        for col, typ in [
            ("data_inicio", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
            ("data_fim", "TIMESTAMPTZ"),
            ("ultima_data_validada", "TIMESTAMPTZ"),
            ("id_inicial", "BIGINT"),
            ("id_final", "BIGINT"),
            ("id_atual", "BIGINT"),
            ("id_atual_merged", "BIGINT"),
        ]:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} " + typ).format(
                    sql.Identifier(schema), sql.Identifier(control), sql.Identifier(col)
                )
            )

    conn.commit()


def get_or_bootstrap_control_row(conn, cfg: Settings) -> Optional[Tuple[str, int, int, int]]:
    """
    Retorna (ctid, id_inicial, id_final, id_atual_merged) de uma linha aberta (data_fim IS NULL),
    já travada FOR UPDATE SKIP LOCKED.

    Se não existir e houver envs ID_ATUAL_MERGED/ID_FINAL_MERGED, cria uma linha.
    Se não existir e não houver envs, retorna None.
    """
    schema, control = cfg.db_schema, cfg.control_table
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT ctid::text, id_inicial, id_final, COALESCE(id_atual_merged, id_inicial) AS id_atual_merged
                FROM {}.{}
                WHERE data_fim IS NULL
                ORDER BY data_inicio DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ).format(sql.Identifier(schema), sql.Identifier(control))
        )
        row = cur.fetchone()
        if row:
            ctid_s, id_inicial, id_final, id_atual_merged = row
            if id_inicial is None or id_final is None or id_atual_merged is None:
                raise RuntimeError(
                    f"Linha aberta em {schema}.{control} existe mas faltam campos (id_inicial/id_final/id_atual_merged)."
                )
            return ctid_s, int(id_inicial), int(id_final), int(id_atual_merged)

        boot_atual = _env("ID_ATUAL_MERGED")
        boot_final = _env("ID_FINAL_MERGED")
        if not (boot_atual and boot_final):
            conn.rollback()
            LOG.info(
                "nenhuma linha aberta em %s.%s e sem envs de bootstrap -> encerrando OK",
                schema,
                control,
            )
            return None

        id_atual = int(boot_atual)
        id_final = int(boot_final)

        cur.execute(
            sql.SQL(
                """
                INSERT INTO {}.{} (data_inicio, data_fim, ultima_data_validada, id_inicial, id_final, id_atual, id_atual_merged)
                VALUES (now(), NULL, NULL, %s, %s, %s, %s)
                """
            ).format(sql.Identifier(schema), sql.Identifier(control)),
            (id_atual, id_final, id_atual, id_atual),
        )

        cur.execute(
            sql.SQL(
                """
                SELECT ctid::text, id_inicial, id_final, COALESCE(id_atual_merged, id_inicial) AS id_atual_merged
                FROM {}.{}
                WHERE data_fim IS NULL
                ORDER BY data_inicio DESC
                LIMIT 1
                FOR UPDATE
                """
            ).format(sql.Identifier(schema), sql.Identifier(control))
        )
        row2 = cur.fetchone()
        if not row2:
            raise RuntimeError("Falha ao bootstrapar linha de controle.")
        ctid_s, id_inicial, id_final, id_atual_merged = row2
        return ctid_s, int(id_inicial), int(id_final), int(id_atual_merged)


def movidesk_get_merged(session: requests.Session, base_url: str, token: str, ticket_id: int, timeout_s: int) -> Optional[Dict[str, Any]]:
    url = f"{base_url}/tickets/merged"
    params = {"token": token, "id": str(ticket_id)}

    try:
        r = session.get(url, params=params, timeout=timeout_s)
    except requests.RequestException as e:
        LOG.warning("falha HTTP ticket_id=%s: %s", ticket_id, e)
        return None

    if r.status_code == 404:
        return None

    if r.status_code == 429:
        ra = r.headers.get("Retry-After")
        try:
            wait_s = float(ra) if ra else 10.0
        except ValueError:
            wait_s = 10.0
        LOG.warning("rate limit (429). aguardando %.1fs ...", wait_s)
        time.sleep(max(0.0, wait_s))
        return None

    if r.status_code >= 400:
        LOG.warning("erro API status=%s ticket_id=%s body=%s", r.status_code, ticket_id, r.text[:200])
        return None

    try:
        data = r.json()
    except ValueError:
        LOG.warning("json inválido ticket_id=%s body=%s", ticket_id, r.text[:200])
        return None

    return data if isinstance(data, dict) else None


def normalize_merged_record(ticket_id: int, data: Dict[str, Any]) -> Optional[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]]:
    tid = data.get("ticketId", ticket_id)
    try:
        tid_i = int(tid)
    except Exception:
        tid_i = ticket_id

    merged_raw = data.get("mergedTickets")
    merged_tickets = int(merged_raw) if str(merged_raw).isdigit() else None

    merged_ids_str = data.get("mergedTicketsIds")
    merged_ids_arr = parse_merged_ids(merged_ids_str)

    last_update = parse_dt(data.get("lastUpdate"))

    if not merged_tickets or merged_tickets <= 0:
        return None

    return (tid_i, merged_tickets, merged_ids_str, merged_ids_arr, last_update)


def upsert_rows(conn, schema: str, table: str, rows: List[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]]) -> int:
    if not rows:
        return 0

    cols = ["ticket_id", "merged_tickets", "merged_tickets_ids", "merged_ticket_ids_arr", "last_update", "synced_at"]
    values = [(r[0], r[1], r[2], r[3], r[4], utcnow()) for r in rows]

    insert_sql = sql.SQL(
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
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
    )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql.as_string(conn), values, page_size=1000)

    return len(rows)


def update_control_progress(conn, cfg: Settings, ctid_s: str, new_id_atual_merged: int, finished: bool) -> None:
    schema, control = cfg.db_schema, cfg.control_table
    with conn.cursor() as cur:
        if finished:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}.{}
                    SET id_atual_merged = %s,
                        ultima_data_validada = now(),
                        data_fim = now()
                    WHERE ctid::text = %s
                    """
                ).format(sql.Identifier(schema), sql.Identifier(control)),
                (new_id_atual_merged, ctid_s),
            )
        else:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}.{}
                    SET id_atual_merged = %s,
                        ultima_data_validada = now()
                    WHERE ctid::text = %s
                    """
                ).format(sql.Identifier(schema), sql.Identifier(control)),
                (new_id_atual_merged, ctid_s),
            )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = load_settings()
    throttle_s = 60.0 / cfg.rpm

    LOG.info(
        "scanner iniciando | schema=%s tabela=%s control=%s | limit=%s | rpm=%s | throttle=%.2fs | dry_run=%s",
        cfg.db_schema, cfg.table_name, cfg.control_table, cfg.limit, cfg.rpm, throttle_s, cfg.dry_run
    )

    conn = pg_connect(cfg.dsn)
    try:
        ensure_tables(conn, cfg.db_schema, cfg.table_name, cfg.control_table)

        row = get_or_bootstrap_control_row(conn, cfg)
        if not row:
            return

        ctid_s, id_inicial, id_final, id_atual_merged = row

        if id_atual_merged < id_final:
            LOG.info("scanner já finalizado (id_atual_merged=%s < id_final=%s). Fechando linha.", id_atual_merged, id_final)
            if not cfg.dry_run:
                update_control_progress(conn, cfg, ctid_s, id_final, finished=True)
                conn.commit()
            else:
                conn.rollback()
            return

        start_id = id_atual_merged
        end_id = max(id_final, start_id - cfg.limit + 1)  # inclusivo
        ids = list(range(start_id, end_id - 1, -1))
        next_id = end_id - 1

        LOG.info("lote | id_atual_merged=%s -> end=%s (n=%s)", start_id, end_id, len(ids))

        session = requests.Session()
        session.headers.update({"User-Agent": "movidesk-sync-range-scan/1.0"})

        to_upsert: List[Tuple[int, Optional[int], Optional[str], Optional[List[int]], Optional[datetime]]] = []
        fetched = 0

        for idx, tid in enumerate(ids):
            data = movidesk_get_merged(session, cfg.base_url, cfg.movi_token, tid, cfg.timeout_s)
            fetched += 1
            if data:
                rec = normalize_merged_record(tid, data)
                if rec:
                    to_upsert.append(rec)

            if idx < len(ids) - 1:
                time.sleep(throttle_s)

        if cfg.dry_run:
            LOG.info("dry_run=True | fetched=%s to_upsert=%s | não gravou nada", fetched, len(to_upsert))
            conn.rollback()
            return

        upserted = upsert_rows(conn, cfg.db_schema, cfg.table_name, to_upsert)

        finished = next_id < id_final
        new_progress = id_final if finished else next_id
        update_control_progress(conn, cfg, ctid_s, new_progress, finished=finished)

        conn.commit()
        LOG.info("scanner concluído | fetched=%s upserted=%s | novo_id_atual_merged=%s | finished=%s",
                 fetched, upserted, new_progress, finished)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
