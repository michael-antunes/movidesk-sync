import os
import sys
import time
import logging
from datetime import datetime, timezone

import psycopg2


DEFAULT_SCHEMA = "visualizacao_resolvidos"
CONTROL_TABLE = "range_scan_control"

# Fonte preferida para descobrir limites (segue seu banco / prints)
PREFERRED_SOURCE_TABLES = [
    ("visualizacao_resolvidos", "tickets_resolvidos_detail"),
    ("visualizacao_resolvidos", "tickets_resolvidos"),
]


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("kickoff")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    # aceita "Z"
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
    return psycopg2.connect(
        dsn,
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "15")),
        application_name=os.getenv("PGAPPNAME", "movidesk-kickoff"),
        keepalives=1,
        keepalives_idle=int(os.getenv("PGKEEPALIVES_IDLE", "30")),
        keepalives_interval=int(os.getenv("PGKEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PGKEEPALIVES_COUNT", "5")),
    )


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            """,
            [schema, table],
        )
        return cur.fetchone() is not None


def get_table_columns(conn, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            [schema, table],
        )
        return {r[0] for r in cur.fetchall()}


def find_raw_column(cols: set[str]) -> str | None:
    for c in ("raw", "raw_json", "payload", "data"):
        if c in cols:
            return c
    return None


def ensure_schema_and_control_table(conn, schema: str, logger: logging.Logger):
    """
    Cria a tabela se não existir, e garante colunas conforme seu print.
    Não mexe no CHECK existente se já existir.
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Cria base (se não existir)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{CONTROL_TABLE} (
              data_inicio timestamptz NOT NULL,
              data_fim timestamptz NOT NULL,
              ultima_data_validada timestamptz,
              id_inicial bigint NOT NULL,
              id_final bigint NOT NULL,
              id_atual bigint,
              id_atual_merged bigint,
              concluido_em timestamptz,
              created_at timestamptz NOT NULL DEFAULT now(),
              updated_at timestamptz NOT NULL DEFAULT now(),
              ultima_data_validada_merged timestamptz
            )
            """
        )

        # Garante colunas do seu banco (se a tabela já existia diferente)
        for coldef in [
            "ADD COLUMN IF NOT EXISTS data_inicio timestamptz",
            "ADD COLUMN IF NOT EXISTS data_fim timestamptz",
            "ADD COLUMN IF NOT EXISTS ultima_data_validada timestamptz",
            "ADD COLUMN IF NOT EXISTS id_inicial bigint",
            "ADD COLUMN IF NOT EXISTS id_final bigint",
            "ADD COLUMN IF NOT EXISTS id_atual bigint",
            "ADD COLUMN IF NOT EXISTS id_atual_merged bigint",
            "ADD COLUMN IF NOT EXISTS concluido_em timestamptz",
            "ADD COLUMN IF NOT EXISTS created_at timestamptz",
            "ADD COLUMN IF NOT EXISTS updated_at timestamptz",
            "ADD COLUMN IF NOT EXISTS ultima_data_validada_merged timestamptz",
        ]:
            cur.execute(f"ALTER TABLE {schema}.{CONTROL_TABLE} {coldef}")

    logger.info(f"kickoff: control table OK -> {schema}.{CONTROL_TABLE}")


def pick_source_table(conn, logger: logging.Logger) -> tuple[str, str] | None:
    for sch, tbl in PREFERRED_SOURCE_TABLES:
        if table_exists(conn, sch, tbl):
            logger.info(f"kickoff: fonte encontrada: {sch}.{tbl}")
            return sch, tbl
    logger.info("kickoff: nenhuma tabela-fonte encontrada (tickets_resolvidos_detail / tickets_resolvidos).")
    return None


def compute_bounds_from_source(conn, logger: logging.Logger, schema: str, table: str):
    cols = get_table_columns(conn, schema, table)

    # id column
    id_col = None
    for c in ("ticket_id", "id"):
        if c in cols:
            id_col = c
            break
    if not id_col:
        raise RuntimeError(f"Tabela fonte {schema}.{table} não tem coluna ticket_id nem id.")

    raw_col = find_raw_column(cols)

    with conn.cursor() as cur:
        # ids
        cur.execute(f"SELECT MAX({id_col})::bigint, MIN({id_col})::bigint FROM {schema}.{table}")
        max_id, min_id = cur.fetchone()

        # datas (preferência: JSON createdDate, se existir)
        min_dt = None
        max_dt = None
        if raw_col:
            # createdDate é padrão na Movidesk; tentamos extrair do jsonb
            cur.execute(
                f"""
                SELECT
                  MIN( NULLIF(({raw_col}->>'createdDate')::text,'')::timestamptz ),
                  MAX( NULLIF(({raw_col}->>'createdDate')::text,'')::timestamptz )
                FROM {schema}.{table}
                """
            )
            min_dt, max_dt = cur.fetchone()

        # fallback: se não achou datas no JSON, usa janela "agora-5y .. agora"
        if not min_dt or not max_dt:
            end = now_utc()
            start = end.replace(year=max(1970, end.year - 5))
            min_dt = min_dt or start
            max_dt = max_dt or end

    if max_id is None or min_id is None:
        raise RuntimeError(f"Tabela fonte {schema}.{table} está vazia (sem IDs).")

    logger.info(
        f"kickoff: bounds fonte {schema}.{table}: "
        f"ids [{max_id} .. {min_id}] (topo→base), datas [{min_dt} .. {max_dt}]"
    )
    return int(max_id), int(min_id), min_dt, max_dt


def load_current_control(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT data_inicio, data_fim, ultima_data_validada,
                   id_inicial, id_final, id_atual, id_atual_merged,
                   concluido_em
            FROM {schema}.{CONTROL_TABLE}
            ORDER BY created_at DESC NULLS LAST, updated_at DESC NULLS LAST
            LIMIT 1
            """
        )
        return cur.fetchone()


def reset_and_insert_control(conn, logger: logging.Logger, schema: str, row: dict):
    """
    Se a tabela não tiver PK/UNIQUE, o jeito mais seguro é manter apenas 1 linha:
    limpa e insere.
    """
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{CONTROL_TABLE}")

        cur.execute(
            f"""
            INSERT INTO {schema}.{CONTROL_TABLE}
              (data_inicio, data_fim, ultima_data_validada,
               id_inicial, id_final, id_atual, id_atual_merged,
               concluido_em, created_at, updated_at, ultima_data_validada_merged)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s, now(), now(), %s)
            """,
            [
                row["data_inicio"],
                row["data_fim"],
                row["ultima_data_validada"],
                row["id_inicial"],
                row["id_final"],
                row["id_atual"],
                row["id_atual_merged"],
                row["concluido_em"],
                row["ultima_data_validada_merged"],
            ],
        )

    logger.info("kickoff: range_scan_control reiniciado e gravado com 1 linha.")


def main():
    logger = setup_logger()

    dsn = os.getenv("NEON_DSN")
    if not dsn:
        raise SystemExit("NEON_DSN é obrigatório")

    schema = os.getenv("SCAN_SCHEMA", DEFAULT_SCHEMA)
    force_reset = env_bool("FORCE_RESET", False)

    start_env = parse_dt(os.getenv("START_DATE"))
    end_env = parse_dt(os.getenv("END_DATE"))

    with pg_connect(dsn) as conn:
        conn.autocommit = False

        ensure_schema_and_control_table(conn, schema, logger)

        current = load_current_control(conn, schema)
        if current:
            data_inicio, data_fim, ultima_valid, id_ini, id_fim, id_atual, id_atual_m, concluido_em = current
            logger.info(
                f"kickoff: control atual: "
                f"data_inicio={data_inicio}, data_fim={data_fim}, ultima_data_validada={ultima_valid}, "
                f"id_inicial={id_ini}, id_final={id_fim}, id_atual={id_atual}, id_atual_merged={id_atual_m}, "
                f"concluido_em={concluido_em}"
            )

            # se já existe "em andamento" e não for reset forçado, não mexe
            if concluido_em is None and not force_reset:
                logger.info("kickoff: já existe um scan em andamento. (FORCE_RESET=false) -> saindo OK.")
                conn.commit()
                return

        src = pick_source_table(conn, logger)
        if not src:
            raise SystemExit("Não encontrei tabela fonte para calcular limites (tickets_resolvidos_detail / tickets_resolvidos).")

        src_schema, src_table = src
        max_id, min_id, min_dt, max_dt = compute_bounds_from_source(conn, logger, src_schema, src_table)

        # datas finais do kickoff:
        data_inicio = start_env or min_dt
        data_fim = end_env or max_dt

        # normaliza: garante data_inicio <= data_fim
        if data_fim < data_inicio:
            data_inicio, data_fim = data_fim, data_inicio

        # IMPORTANTÍSSIMO para passar no ck_range_scan_bounds:
        # ultima_data_validada deve estar dentro do intervalo
        ultima_data_validada = data_inicio
        ultima_data_validada_merged = data_inicio

        row = {
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "ultima_data_validada": ultima_data_validada,
            "ultima_data_validada_merged": ultima_data_validada_merged,
            "id_inicial": max_id,          # topo
            "id_final": min_id,            # base
            "id_atual": None,              # igual seu print (NULL)
            "id_atual_merged": max_id,     # igual seu print (começa no topo)
            "concluido_em": None,
        }

        reset_and_insert_control(conn, logger, schema, row)

        conn.commit()
        logger.info(
            f"kickoff: OK. Range: ids {row['id_inicial']}..{row['id_final']} | "
            f"datas {row['data_inicio']}..{row['data_fim']} | "
            f"ultima_data_validada={row['ultima_data_validada']}"
        )


if __name__ == "__main__":
    main()
