import os
import sys
import time
import logging
from datetime import datetime, timezone

import psycopg2


DEFAULT_SCHEMA = "visualizacao_resolvidos"
CONTROL_TABLE = "range_scan_control"

PREFERRED_SOURCE_TABLES = [
    ("visualizacao_resolvidos", "tickets_resolvidos_detail"),
]


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("kickoff")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    if not logger.handlers:
        logger.addHandler(h)
    return logger


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def pg_connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_control_table(cur, schema: str, table: str):
    cur.execute(
        f"""
        create table if not exists {schema}.{table} (
            id bigserial primary key,
            schema_name text not null,
            table_name text not null,
            mode text not null,
            dt_start timestamptz null,
            dt_end timestamptz null,
            id_inicial bigint null,
            id_final bigint null,
            id_atual bigint null,
            id_atual_merged bigint null,
            status text not null default 'pending',
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now(),
            note text null
        )
        """
    )


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        select exists(
            select 1
              from information_schema.tables
             where table_schema = %s
               and table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cur.fetchone()[0])


def get_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        select column_name
          from information_schema.columns
         where table_schema = %s
           and table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def find_source_table(cur, logger: logging.Logger):
    for schema, table in PREFERRED_SOURCE_TABLES:
        if table_exists(cur, schema, table):
            logger.info("kickoff: tabela-fonte encontrada: %s.%s", schema, table)
            return schema, table
    logger.info("kickoff: nenhuma tabela-fonte encontrada (tickets_resolvidos_detail).")
    return None, None


def choose_id_column(cols: set[str]) -> str:
    for c in ("ticket_id", "id"):
        if c in cols:
            return c
    raise RuntimeError("Tabela fonte não tem coluna ticket_id nem id.")


def choose_raw_column(cols: set[str]) -> str | None:
    for c in ("raw_payload", "raw", "payload", "data"):
        if c in cols:
            return c
    return None


def choose_dt_column(cols: set[str]) -> str | None:
    for c in ("created_at", "updated_at", "last_update", "last_updated_at"):
        if c in cols:
            return c
    return None


def compute_limits_from_source(cur, schema: str, table: str, cols: set[str], logger: logging.Logger):
    id_col = choose_id_column(cols)
    raw_col = choose_raw_column(cols)
    dt_col = choose_dt_column(cols)

    cur.execute(f"select max({id_col})::bigint, min({id_col})::bigint from {schema}.{table}")
    max_id, min_id = cur.fetchone()

    if max_id is None or min_id is None:
        raise RuntimeError(f"Tabela fonte {schema}.{table} está vazia (sem IDs).")

    min_dt = None
    max_dt = None

    if dt_col:
        cur.execute(
            f"""
            select
              min({dt_col})::timestamptz,
              max({dt_col})::timestamptz
            from {schema}.{table}
            """
        )
        min_dt, max_dt = cur.fetchone()

    if (min_dt is None or max_dt is None) and raw_col:
        cur.execute(
            f"""
            select
              min(nullif(({raw_col}->>'createdDate')::text,'')::timestamptz),
              max(nullif(({raw_col}->>'createdDate')::text,'')::timestamptz)
            from {schema}.{table}
            """
        )
        rmin, rmax = cur.fetchone()
        min_dt = min_dt or rmin
        max_dt = max_dt or rmax

    if min_dt is None or max_dt is None:
        now = utcnow()
        min_dt = min_dt or now.replace(year=now.year - 5)
        max_dt = max_dt or now

    logger.info(
        "kickoff: bounds fonte %s.%s: ids [%s .. %s] (max→min), datas [%s .. %s]",
        schema,
        table,
        int(max_id),
        int(min_id),
        min_dt,
        max_dt,
    )
    return int(max_id), int(min_id), min_dt, max_dt


def upsert_range(cur, schema_ctl: str, table_ctl: str, schema_src: str, table_src: str, mode: str, dt_start, dt_end, min_id, max_id, note: str | None):
    cur.execute(
        f"""
        insert into {schema_ctl}.{table_ctl} (
            schema_name, table_name, mode, dt_start, dt_end,
            id_inicial, id_final, id_atual, id_atual_merged,
            status, note, created_at, updated_at
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s,now(),now())
        """,
        (
            schema_src,
            table_src,
            mode,
            dt_start,
            dt_end,
            max_id,
            min_id,
            None,
            max_id,
            note,
        ),
    )


def main():
    logger = setup_logger()

    dsn = os.getenv("NEON_DSN")
    if not dsn:
        raise SystemExit("NEON_DSN não definido")

    schema_ctl = os.getenv("CONTROL_SCHEMA", DEFAULT_SCHEMA)
    table_ctl = os.getenv("CONTROL_TABLE", CONTROL_TABLE)

    mode = os.getenv("MODE", "day")
    note = os.getenv("NOTE")

    lock_seconds = int(os.getenv("LOCK_SECONDS", "0") or "0")
    reset_force = env_bool("RESET_FORCE", False)

    with pg_connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            ensure_control_table(cur, schema_ctl, table_ctl)

            if lock_seconds > 0:
                cur.execute("select pg_advisory_lock(hashtext(%s))", (f"{schema_ctl}.{table_ctl}",))

            schema_src, table_src = find_source_table(cur, logger)
            if not schema_src or not table_src:
                raise SystemExit("Não encontrei tabela fonte para descobrir limites (tickets_resolvidos_detail).")

            cols = get_columns(cur, schema_src, table_src)
            max_id, min_id, min_dt, max_dt = compute_limits_from_source(cur, schema_src, table_src, cols, logger)

            dt_start_env = parse_iso(os.getenv("DT_START"))
            dt_end_env = parse_iso(os.getenv("DT_END"))
            if dt_start_env:
                min_dt = dt_start_env
            if dt_end_env:
                max_dt = dt_end_env

            if not reset_force:
                cur.execute(
                    f"""
                    select 1
                      from {schema_ctl}.{table_ctl}
                     where status in ('running','pending')
                     limit 1
                    """
                )
                if cur.fetchone():
                    logger.info("kickoff: já existe range_scan_control pendente/em andamento; não criando novo range.")
                    return

            upsert_range(cur, schema_ctl, table_ctl, schema_src, table_src, mode, min_dt, max_dt, min_id, max_id, note)
            logger.info("kickoff: range criado em %s.%s (mode=%s)", schema_ctl, table_ctl, mode)

            if lock_seconds > 0:
                time.sleep(lock_seconds)
                cur.execute("select pg_advisory_unlock(hashtext(%s))", (f"{schema_ctl}.{table_ctl}",))


if __name__ == "__main__":
    main()
