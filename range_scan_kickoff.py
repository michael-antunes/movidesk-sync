import os
import sys
import logging
from datetime import datetime, timezone

import psycopg2


SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
CONTROL_TABLE = os.getenv("CONTROL_TABLE", "range_scan_control")
DATA_FIM = datetime(2020, 1, 1, tzinfo=timezone.utc)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("kickoff")
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    if not logger.handlers:
        logger.addHandler(h)
    return logger


def pg_connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_schema_and_control_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL_TABLE} (
                data_fim timestamptz NOT NULL,
                data_inicio timestamptz NOT NULL,
                ultima_data_validada timestamptz,
                id_inicial bigint,
                id_final bigint,
                id_atual bigint,
                id_atual_merged bigint,
                concluido_em timestamptz,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                ultima_data_validada_merged timestamptz
            )
            """
        )
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS data_fim timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS data_inicio timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS ultima_data_validada timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS id_inicial bigint")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS id_final bigint")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS id_atual bigint")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS id_atual_merged bigint")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS concluido_em timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS created_at timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS updated_at timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ADD COLUMN IF NOT EXISTS ultima_data_validada_merged timestamptz")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ALTER COLUMN created_at SET DEFAULT now()")
        cur.execute(f"ALTER TABLE {SCHEMA}.{CONTROL_TABLE} ALTER COLUMN updated_at SET DEFAULT now()")


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1
                  FROM information_schema.tables
                 WHERE table_schema=%s
                   AND table_name=%s
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def get_columns(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema=%s
               AND table_name=%s
            """,
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}


def pick_source_table(conn, logger: logging.Logger):
    if table_exists(conn, "visualizacao_resolvidos", "tickets_resolvidos_detail"):
        logger.info("tabela-fonte encontrada: visualizacao_resolvidos.tickets_resolvidos_detail")
        return "visualizacao_resolvidos", "tickets_resolvidos_detail"
    raise RuntimeError("Tabela fonte não encontrada: visualizacao_resolvidos.tickets_resolvidos_detail")


def compute_id_bounds(conn, schema: str, table: str) -> tuple[int, int]:
    cols = get_columns(conn, schema, table)
    id_col = "ticket_id" if "ticket_id" in cols else ("id" if "id" in cols else None)
    if not id_col:
        raise RuntimeError(f"Tabela fonte {schema}.{table} não tem coluna ticket_id nem id.")
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({id_col})::bigint, MIN({id_col})::bigint FROM {schema}.{table}")
        max_id, min_id = cur.fetchone()
    if max_id is None or min_id is None:
        raise RuntimeError(f"Tabela fonte {schema}.{table} está vazia.")
    return int(max_id), int(min_id)


def reset_and_insert_control(conn, row: dict):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{CONTROL_TABLE}")
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{CONTROL_TABLE}
              (data_inicio, data_fim, ultima_data_validada,
               id_inicial, id_final, id_atual, id_atual_merged,
               concluido_em, created_at, updated_at, ultima_data_validada_merged)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s, now(), now(), %s)
            """,
            (
                row["data_inicio"],
                row["data_fim"],
                row["ultima_data_validada"],
                row["id_inicial"],
                row["id_final"],
                row["id_atual"],
                row["id_atual_merged"],
                row["concluido_em"],
                row["ultima_data_validada_merged"],
            ),
        )


def main():
    logger = setup_logger()
    dsn = os.getenv("NEON_DSN")
    if not dsn:
        raise SystemExit("NEON_DSN não definido")
    with pg_connect(dsn) as conn:
        conn.autocommit = False
        ensure_schema_and_control_table(conn)
        src_schema, src_table = pick_source_table(conn, logger)
        max_id, min_id = compute_id_bounds(conn, src_schema, src_table)
        row = {
            "data_inicio": now_utc(),
            "data_fim": DATA_FIM,
            "ultima_data_validada": None,
            "ultima_data_validada_merged": None,
            "id_inicial": max_id,
            "id_final": min_id,
            "id_atual": None,
            "id_atual_merged": max_id,
            "concluido_em": None,
        }
        reset_and_insert_control(conn, row)
        conn.commit()
        logger.info(
            "OK. data_inicio=%s data_fim=%s id_inicial=%s id_final=%s id_atual=%s id_atual_merged=%s",
            row["data_inicio"],
            row["data_fim"],
            row["id_inicial"],
            row["id_final"],
            row["id_atual"],
            row["id_atual_merged"],
        )


if __name__ == "__main__":
    main()
