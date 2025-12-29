import os
import sys
import logging
from datetime import datetime, timezone

import psycopg2


SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
CONTROL_TABLE = os.getenv("CONTROL_TABLE", "range_scan_control")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "tickets_resolvidos_detail")
DATA_FIM_FIXA = datetime(2020, 1, 1, tzinfo=timezone.utc)


def now_utc():
    return datetime.now(timezone.utc)


def setup_logger():
    logger = logging.getLogger("kickoff")
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    if not logger.handlers:
        logger.addHandler(h)
    return logger


def pg_connect():
    dsn = os.getenv("NEON_DSN")
    if not dsn:
        raise RuntimeError("NEON_DSN não definido")
    return psycopg2.connect(dsn)


def qname(schema, table):
    return f'"{schema}"."{table}"'


def ensure_control_table(cur):
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qname(SCHEMA, CONTROL_TABLE)} (
          data_fim timestamptz NOT NULL,
          data_inicio timestamptz NOT NULL,
          ultima_data_validada timestamptz,
          id_inicial bigint NOT NULL,
          id_final bigint NOT NULL,
          id_atual bigint,
          id_atual_merged bigint
        )
        """
    )
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS data_fim timestamptz")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS data_inicio timestamptz")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS ultima_data_validada timestamptz")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS id_inicial bigint")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS id_final bigint")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS id_atual bigint")
    cur.execute(f"ALTER TABLE {qname(SCHEMA, CONTROL_TABLE)} ADD COLUMN IF NOT EXISTS id_atual_merged bigint")


def get_bounds(cur):
    cur.execute(
        f"""
        SELECT MAX(ticket_id)::bigint, MIN(ticket_id)::bigint
        FROM {qname(SCHEMA, SOURCE_TABLE)}
        """
    )
    mx, mn = cur.fetchone()
    if mx is None or mn is None:
        raise RuntimeError(f"{SCHEMA}.{SOURCE_TABLE} está vazia")
    return int(mx), int(mn)


def main():
    logger = setup_logger()
    with pg_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            ensure_control_table(cur)
            id_inicial, id_final = get_bounds(cur)

            data_inicio = now_utc()
            data_fim = DATA_FIM_FIXA

            cur.execute(f"DELETE FROM {qname(SCHEMA, CONTROL_TABLE)}")
            cur.execute(
                f"""
                INSERT INTO {qname(SCHEMA, CONTROL_TABLE)}
                  (data_fim, data_inicio, ultima_data_validada, id_inicial, id_final, id_atual, id_atual_merged)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s)
                """,
                (data_fim, data_inicio, None, id_inicial, id_final, None, id_inicial),
            )

            logger.info(
                "OK range_scan_control: data_inicio=%s data_fim=%s id_inicial=%s id_final=%s id_atual=%s id_atual_merged=%s",
                data_inicio,
                data_fim,
                id_inicial,
                id_final,
                None,
                id_inicial,
            )


if __name__ == "__main__":
    main()
