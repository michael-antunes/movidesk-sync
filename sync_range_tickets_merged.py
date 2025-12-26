#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import psycopg2
import psycopg2.extras

LOG = logging.getLogger("range_scan")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "t", "yes", "y", "on")


def _env_float(name: str, default: float) -> float:
    v = _env(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(float(v))
    except ValueError:
        return default


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s[:10])


def pg_connect() -> "psycopg2.extensions.connection":
    # Suporta: DATABASE_URL, NEON_DSN, ou PG*.
    database_url = _env("DATABASE_URL")
    neon_dsn = _env("NEON_DSN")

    if database_url:
        return psycopg2.connect(database_url)
    if neon_dsn:
        return psycopg2.connect(neon_dsn)

    host = _env("PGHOST")
    dbname = _env("PGDATABASE")
    user = _env("PGUSER")
    password = _env("PGPASSWORD")
    port = _env("PGPORT", "5432")
    sslmode = _env("PGSSLMODE")

    if not all([host, dbname, user, password]):
        raise RuntimeError(
            "Faltam variáveis de Postgres. Use DATABASE_URL ou NEON_DSN ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

    kw: Dict[str, Any] = dict(host=host, dbname=dbname, user=user, password=password, port=port)
    if sslmode:
        kw["sslmode"] = sslmode
    return psycopg2.connect(**kw)


@dataclass
class Settings:
    api_base: str
    token: str

    db_schema: str
    table_name: str
    control_table: str
    resolved_table: str

    resolved_id_col: Optional[str]
    resolved_date_col: Optional[str]

    limit: int
    rpm: float
    dry_run: bool

    bootstrap_start: Optional[str]
    bootstrap_end: Optional[str]

    @property
    def throttle_seconds(self) -> float:
        return 0.0 if self.rpm <= 0 else 60.0 / self.rpm


def load_settings() -> Settings:
    # compatível com versões antigas
    token = _env("MOVIDESK_TOKEN") or _env("MOVI_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVIDESK_TOKEN (token da API Movidesk).")

    return Settings(
        api_base=_env("API_BASE", "https://api.movidesk.com/public/v1"),
        token=token,
        db_schema=_env("DB_SCHEMA", "visualizacao_resolvidos"),
        table_name=_env("TABLE_NAME", "tickets_mesclados"),
        control_table=_env("CONTROL_TABLE", "range_scan_control"),
        resolved_table=_env("RESOLVED_TABLE", "tickets_resolvidos_detail"),
        resolved_id_col=_env("RESOLVED_ID_COL"),
        resolved_date_col=_env("RESOLVED_DATE_COL"),
        limit=_env_int("LIMIT", 80),
        rpm=_env_float("RPM", 9.0),
        dry_run=_env_bool("DRY_RUN", False),
        bootstrap_start=_env("BOOTSTRAP_START_DATE"),
        bootstrap_end=_env("BOOTSTRAP_END_DATE"),
    )


def ensure_tables(conn, schema: str, tickets_table: str, control_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        # mesma estrutura do original
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."{tickets_table}" (
              ticket_id BIGINT PRIMARY KEY,
              merged_tickets INT,
              merged_tickets_ids TEXT,
              merged_ticket_ids_arr BIGINT[],
              last_update TIMESTAMPTZ,
              synced_at TIMESTAMPTZ
            );
            '''
        )

        # auditoria
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."audit_recent_run" (
              run_at TIMESTAMPTZ DEFAULT now(),
              data_inicio TIMESTAMPTZ,
              data_fim TIMESTAMPTZ,
              pointer_before BIGINT,
              pointer_after BIGINT,
              selected_ids INT,
              skipped_existing INT,
              api_calls INT,
              merged_found INT,
              upserted INT,
              errors INT,
              duration_ms INT
            );
            '''
        )

        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."audit_recent_missing" (
              seen_at TIMESTAMPTZ DEFAULT now(),
              ticket_id BIGINT,
              merged_tickets INT,
              merged_tickets_ids TEXT,
              merged_ticket_ids_arr BIGINT[]
            );
            '''
        )

        # control (igual ao seu print, sem id)
        cur.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."{control_table}" (
              data_inicio TIMESTAMPTZ,
              data_fim TIMESTAMPTZ,
              ultima_data_validada TIMESTAMPTZ,
              id_inicial BIGINT,
              id_final BIGINT,
              id_atual BIGINT,
              id_atual_merged BIGINT
            );
            '''
        )

        # garante colunas
        for col, ctype in [
            ("data_inicio", "TIMESTAMPTZ"),
            ("data_fim", "TIMESTAMPTZ"),
            ("ultima_data_validada", "TIMESTAMPTZ"),
            ("id_inicial", "BIGINT"),
            ("id_final", "BIGINT"),
            ("id_atual", "BIGINT"),
            ("id_atual_merged", "BIGINT"),
        ]:
            cur.execute(f'ALTER TABLE "{schema}"."{control_table}" ADD COLUMN IF NOT EXISTS {col} {ctype};')

    conn.commit()


def list_columns(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


def choose_resolved_columns(conn, schema: str, resolved_table: str,
                           id_col: Optional[str], date_col: Optional[str]) -> Tuple[str, str]:
    cols = set(c.lower() for c in list_columns(conn, schema, resolved_table))

    # id
    if id_col:
        if id_col.lower() not in cols:
            raise RuntimeError(f"RESOLVED_ID_COL='{id_col}' não existe em {schema}.{resolved_table}.")
        chosen_id = id_col
    else:
        for cand in ["ticket_id", "id", "ticketid", "codigo", "ticket"]:
            if cand in cols:
                chosen_id = cand
                break
        else:
            raise RuntimeError(f"Não consegui inferir a coluna de ID em {schema}.{resolved_table}. Defina RESOLVED_ID_COL.")

    # data
    if date_col:
        if date_col.lower() not in cols:
            raise RuntimeError(f"RESOLVED_DATE_COL='{date_col}' não existe em {schema}.{resolved_table}.")
        chosen_date = date_col
    else:
        for cand in ["resolved_date", "resolved_at", "resolved_datetime", "data_resolvido",
                     "data_resolucao", "resolveddate", "data_fechamento", "closed_at"]:
            if cand in cols:
                chosen_date = cand
                break
        else:
            raise RuntimeError(f"Não consegui inferir a coluna de data em {schema}.{resolved_table}. Defina RESOLVED_DATE_COL.")

    return chosen_id, chosen_date


def read_control_row(conn, schema: str, control_table: str) -> Optional[Dict[str, Any]]:
    cols = set(c.lower() for c in list_columns(conn, schema, control_table))
    has_id = "id" in cols
    has_done = "concluido_em" in cols

    select_cols: List[str] = []
    if has_id:
        select_cols.append("id")
    select_cols.append("ctid::text AS row_ctid")

    for c in ["data_inicio", "data_fim", "ultima_data_validada", "id_inicial",
              "id_final", "id_atual", "id_atual_merged"]:
        if c.lower() in cols:
            select_cols.append(c)

    where = "WHERE concluido_em IS NULL" if has_done else ""
    sql = f'''
        SELECT {", ".join(select_cols)}
        FROM "{schema}"."{control_table}"
        {where}
        ORDER BY data_inicio DESC NULLS LAST, data_fim DESC NULLS LAST
        LIMIT 1;
    '''
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return dict(row) if row else None


def update_control_row(conn, schema: str, control_table: str, row: Dict[str, Any], updates: Dict[str, Any]) -> None:
    cols = set(c.lower() for c in list_columns(conn, schema, control_table))
    has_id = "id" in cols

    safe_updates = {k: v for k, v in updates.items() if k.lower() in cols}
    if not safe_updates:
        return

    set_sql = ", ".join([f"{k} = %s" for k in safe_updates.keys()])
    params = list(safe_updates.values())

    if has_id and "id" in row:
        where_sql = "WHERE id = %s"
        params.append(row["id"])
    else:
        where_sql = "WHERE ctid::text = %s"
        params.append(row["row_ctid"])

    sql = f'UPDATE "{schema}"."{control_table}" SET {set_sql} {where_sql};'
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def bootstrap_control_row(conn, cfg: Settings, resolved_id_col: str, resolved_date_col: str) -> Optional[Dict[str, Any]]:
    if not (cfg.bootstrap_start and cfg.bootstrap_end):
        return None

    start = _parse_date(cfg.bootstrap_start)
    end = _parse_date(cfg.bootstrap_end)

    with conn.cursor() as cur:
        cur.execute(
            f'''
            SELECT MAX({resolved_id_col}) AS id_inicial, MIN({resolved_id_col}) AS id_final
            FROM "{cfg.db_schema}"."{cfg.resolved_table}"
            WHERE {resolved_date_col}::date >= %s AND {resolved_date_col}::date <= %s
            ''',
            (start, end),
        )
        r = cur.fetchone()
        id_inicial, id_final = (r[0], r[1]) if r else (None, None)

        if id_inicial is None or id_final is None:
            LOG.warning("bootstrap: não encontrei tickets em %s..%s", start, end)
            return None

        cur.execute(
            f'''
            INSERT INTO "{cfg.db_schema}"."{cfg.control_table}"
            (data_inicio, data_fim, ultima_data_validada, id_inicial, id_final, id_atual, id_atual_merged)
            VALUES (%s, %s, now(), %s, %s, %s, %s)
            ''',
            (start, end, id_inicial, id_final, id_inicial, id_inicial),
        )

    conn.commit()
    return read_control_row(conn, cfg.db_schema, cfg.control_table)


def fetch_existing_ticket_ids(conn, schema: str, tickets_table: str, ids: Sequence[int]) -> set:
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT ticket_id FROM "{schema}"."{tickets_table}" WHERE ticket_id = ANY(%s);',
            (list(ids),),
        )
        return {r[0] for r in cur.fetchall()}


def select_next_ids(conn, schema: str, resolved_table: str, resolved_id_col: str, resolved_date_col: str,
                    start_ts, end_ts, pointer: int, id_final: int, limit: int) -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            f'''
            SELECT {resolved_id_col}
            FROM "{schema}"."{resolved_table}"
            WHERE {resolved_date_col} >= %s
              AND {resolved_date_col} <= %s
              AND {resolved_id_col} <= %s
              AND {resolved_id_col} >= %s
            ORDER BY {resolved_id_col} DESC
            LIMIT %s
            ''',
            (start_ts, end_ts, pointer, id_final, limit),
        )
        return [int(r[0]) for r in cur.fetchall()]


def api_get_ticket_merged(cfg: Settings, ticket_id: int, session: requests.Session) -> Optional[Dict[str, Any]]:
    url = f"{cfg.api_base.rstrip('/')}/tickets/merged"
    params = {"token": cfg.token, "id": str(ticket_id)}
    resp = session.get(url, params=params, timeout=30)

    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise RuntimeError(f"API {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    if data is None:
        return None
    if isinstance(data, list):
        return data[0] if data and isinstance(data[0], dict) else None
    if isinstance(data, dict):
        return data
    return None


def parse_merged_ids(val: Any) -> List[int]:
    if val is None:
        return []
    if isinstance(val, list):
        out: List[int] = []
        for x in val:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out

    s = str(val).strip()
    if not s:
        return []
    parts = [p for p in s.replace(",", ";").split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p.strip()))
        except ValueError:
            pass
    return out


def upsert_ticket_merged(conn, schema: str, tickets_table: str, row: Dict[str, Any], dry_run: bool) -> int:
    sql = f'''
        INSERT INTO "{schema}"."{tickets_table}"
          (ticket_id, merged_tickets, merged_tickets_ids, merged_ticket_ids_arr, last_update, synced_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (ticket_id) DO UPDATE SET
          merged_tickets = EXCLUDED.merged_tickets,
          merged_tickets_ids = EXCLUDED.merged_tickets_ids,
          merged_ticket_ids_arr = EXCLUDED.merged_ticket_ids_arr,
          last_update = EXCLUDED.last_update,
          synced_at = now();
    '''
    if dry_run:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                row["ticket_id"],
                row.get("merged_tickets"),
                row.get("merged_tickets_ids"),
                row.get("merged_ticket_ids_arr"),
                row.get("last_update"),
            ),
        )
    conn.commit()
    return 1


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = load_settings()
    conn = pg_connect()
    try:
        ensure_tables(conn, cfg.db_schema, cfg.table_name, cfg.control_table)
        resolved_id_col, resolved_date_col = choose_resolved_columns(
            conn, cfg.db_schema, cfg.resolved_table, cfg.resolved_id_col, cfg.resolved_date_col
        )

        control = read_control_row(conn, cfg.db_schema, cfg.control_table)
        if not control:
            control = bootstrap_control_row(conn, cfg, resolved_id_col, resolved_date_col)

        if not control:
            LOG.info("nenhuma linha em %s.%s e sem BOOTSTRAP_* -> encerrando OK", cfg.db_schema, cfg.control_table)
            return

        data_inicio = control.get("data_inicio")
        data_fim = control.get("data_fim")
        if not data_inicio or not data_fim:
            raise RuntimeError("Control table precisa ter data_inicio e data_fim preenchidos.")

        id_inicial = control.get("id_inicial")
        id_final = control.get("id_final")

        # se não tiver id_inicial/id_final, calcula via resolved_detail
        if id_inicial is None or id_final is None:
            with conn.cursor() as cur:
                cur.execute(
                    f'''
                    SELECT MAX({resolved_id_col}) AS id_inicial, MIN({resolved_id_col}) AS id_final
                    FROM "{cfg.db_schema}"."{cfg.resolved_table}"
                    WHERE {resolved_date_col} >= %s AND {resolved_date_col} <= %s
                    ''',
                    (data_inicio, data_fim),
                )
                r = cur.fetchone()
                id_inicial, id_final = (r[0], r[1]) if r else (None, None)

            if id_inicial is None or id_final is None:
                LOG.info("Sem tickets no resolved_detail dentro do range -> OK")
                return

            update_control_row(conn, cfg.db_schema, cfg.control_table, control, {"id_inicial": id_inicial, "id_final": id_final})

        pointer = control.get("id_atual_merged") or control.get("id_atual") or id_inicial

        LOG.info(
            "scanner iniciando | schema=%s tabela=%s control=%s | range=%s..%s | pointer=%s | id_final=%s | limit=%s | rpm=%s | throttle=%.2fs | dry_run=%s",
            cfg.db_schema, cfg.table_name, cfg.control_table, data_inicio, data_fim, pointer, id_final,
            cfg.limit, cfg.rpm, cfg.throttle_seconds, cfg.dry_run
        )

        if int(pointer) < int(id_final):
            LOG.info("pointer já passou do id_final -> OK")
            return

        t0 = time.time()
        ids = select_next_ids(
            conn, cfg.db_schema, cfg.resolved_table, resolved_id_col, resolved_date_col,
            data_inicio, data_fim, int(pointer), int(id_final), cfg.limit
        )
        if not ids:
            LOG.info("não há mais ids para processar no range -> OK")
            return

        existing = fetch_existing_ticket_ids(conn, cfg.db_schema, cfg.table_name, ids)

        api_calls = skipped = merged_found = upserted = errors = 0
        session = requests.Session()

        for i, tid in enumerate(ids):
            if tid in existing:
                skipped += 1
                continue

            try:
                api_calls += 1
                obj = api_get_ticket_merged(cfg, tid, session)
                if obj:
                    merged_ids = parse_merged_ids(obj.get("mergedTicketsIds"))
                    merged_tickets = obj.get("mergedTickets")
                    if merged_tickets is None:
                        merged_tickets = len(merged_ids)
                    merged_tickets = int(merged_tickets) if merged_tickets is not None else 0

                    if merged_tickets > 0 or merged_ids:
                        merged_found += 1
                        row = dict(
                            ticket_id=int(obj.get("ticketId") or tid),
                            merged_tickets=merged_tickets,
                            merged_tickets_ids=str(obj.get("mergedTicketsIds") or ""),
                            merged_ticket_ids_arr=merged_ids,
                            last_update=dt.datetime.now(dt.timezone.utc),
                        )
                        upserted += upsert_ticket_merged(conn, cfg.db_schema, cfg.table_name, row, cfg.dry_run)

                        if not cfg.dry_run:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f'''
                                    INSERT INTO "{cfg.db_schema}"."audit_recent_missing"
                                    (ticket_id, merged_tickets, merged_tickets_ids, merged_ticket_ids_arr)
                                    VALUES (%s, %s, %s, %s)
                                    ''',
                                    (row["ticket_id"], row["merged_tickets"], row["merged_tickets_ids"], row["merged_ticket_ids_arr"]),
                                )
                            conn.commit()

            except Exception as e:
                errors += 1
                LOG.warning("erro validando ticket_id=%s: %s", tid, e)

            if cfg.throttle_seconds > 0 and i < len(ids) - 1:
                time.sleep(cfg.throttle_seconds)

        pointer_after = min(ids) - 1
        update_control_row(
            conn, cfg.db_schema, cfg.control_table, control,
            {
                "ultima_data_validada": dt.datetime.now(dt.timezone.utc),
                "id_atual": pointer_after,
                "id_atual_merged": pointer_after,
            }
        )

        dur_ms = int((time.time() - t0) * 1000)
        if not cfg.dry_run:
            with conn.cursor() as cur:
                cur.execute(
                    f'''
                    INSERT INTO "{cfg.db_schema}"."audit_recent_run"
                    (data_inicio, data_fim, pointer_before, pointer_after, selected_ids, skipped_existing, api_calls, merged_found, upserted, errors, duration_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ''',
                    (data_inicio, data_fim, int(pointer), int(pointer_after), len(ids), skipped, api_calls, merged_found, upserted, errors, dur_ms),
                )
            conn.commit()

        LOG.info(
            "scanner concluído | ids=%s (skipped=%s, api_calls=%s) | merged_found=%s | upserted=%s | errors=%s | pointer_after=%s | %sms",
            len(ids), skipped, api_calls, merged_found, upserted, errors, pointer_after, dur_ms
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
