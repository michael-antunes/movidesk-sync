import os
from datetime import datetime, timezone

import psycopg2


SCHEMA = os.getenv("RANGE_SCHEMA", "visualizacao_resolvidos")
DETAIL_TABLE = os.getenv("RANGE_DETAIL_TABLE", "tickets_resolvidos_detail")
CONTROL_TABLE = os.getenv("RANGE_CONTROL_TABLE", "range_scan_control")

BASE_STATUSES = ("Resolved", "Closed", "Canceled")


def get_columns_info(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            [schema, table],
        )
        rows = cur.fetchall()
    info = {}
    for name, dtype, is_nullable, default in rows:
        info[name] = {
            "data_type": (dtype or "").lower(),
            "nullable": (is_nullable == "YES"),
            "default": default,
        }
    return info


def main():
    dsn = os.getenv("NEON_DSN")
    if not dsn:
        raise SystemExit("NEON_DSN é obrigatório")

    conn = psycopg2.connect(
        dsn,
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "15")),
        application_name=os.getenv("PGAPPNAME", "movidesk-range-kickoff"),
        keepalives=1,
        keepalives_idle=int(os.getenv("PGKEEPALIVES_IDLE", "30")),
        keepalives_interval=int(os.getenv("PGKEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PGKEEPALIVES_COUNT", "5")),
    )

    now = datetime.now(timezone.utc)

    try:
        conn.autocommit = False

        with conn.cursor() as cur:
            # 1) Descobrir bounds reais (min/max) do detail
            cur.execute(
                f"""
                SELECT
                  MIN(last_update) AS min_last_update,
                  MAX(last_update) AS max_last_update,
                  MIN(ticket_id)   AS min_id,
                  MAX(ticket_id)   AS max_id
                FROM {SCHEMA}.{DETAIL_TABLE}
                WHERE base_status IN %s
                """,
                [BASE_STATUSES],
            )
            row = cur.fetchone()

        if not row or row[0] is None or row[1] is None or row[2] is None or row[3] is None:
            raise SystemExit(
                f"Não consegui calcular bounds: {SCHEMA}.{DETAIL_TABLE} está vazio "
                f"ou sem last_update/ticket_id/base_status preenchidos."
            )

        min_lu, max_lu, min_id, max_id = row

        # 2) Bounds CORRETOS (seguindo a lógica natural e o CHECK de bounds)
        data_inicio = min_lu
        data_fim = max_lu

        id_inicial = int(min_id)
        id_final = int(max_id)

        # Cursor inicial (se sua lógica preferir NULL, troque para None)
        id_atual = id_inicial
        id_atual_merged = id_inicial

        # 3) Montar insert respeitando as colunas existentes no seu banco
        cols_info = get_columns_info(conn, SCHEMA, CONTROL_TABLE)
        cols = set(cols_info.keys())

        values_map = {}

        # timestamps (as 2 mais importantes)
        if "data_inicio" in cols:
            values_map["data_inicio"] = data_inicio
        if "data_fim" in cols:
            values_map["data_fim"] = data_fim

        # alguns bancos têm data_validada (não usaremos no CHECK normalmente, mas preenchendo ajuda)
        if "data_validada" in cols:
            values_map["data_validada"] = now

        # ids/bounds
        if "id_inicial" in cols:
            values_map["id_inicial"] = id_inicial
        if "id_final" in cols:
            values_map["id_final"] = id_final
        if "id_atual" in cols:
            values_map["id_atual"] = id_atual
        if "id_atual_merged" in cols:
            values_map["id_atual_merged"] = id_atual_merged

        # “últimas validadas”
        if "ultima_data_validada" in cols:
            values_map["ultima_data_validada"] = data_inicio
        if "ultima_data_validada_merged" in cols:
            values_map["ultima_data_validada_merged"] = data_inicio

        # Preencher quaisquer NOT NULL sem default que existam (pra não estourar)
        for col, meta in cols_info.items():
            if col in values_map:
                continue
            if meta["nullable"]:
                continue
            if meta["default"] is not None:
                continue

            dt = meta["data_type"]
            if "timestamp" in dt or dt == "date":
                values_map[col] = now
            elif dt in ("integer", "bigint", "smallint", "numeric", "real", "double precision"):
                values_map[col] = 0
            elif dt == "boolean":
                values_map[col] = False
            else:
                values_map[col] = ""

        insert_cols = list(values_map.keys())
        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_list = ", ".join(insert_cols)
        params = [values_map[c] for c in insert_cols]

        with conn.cursor() as cur:
            # manter só 1 linha de controle
            cur.execute(f"DELETE FROM {SCHEMA}.{CONTROL_TABLE}")

            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{CONTROL_TABLE} ({col_list})
                VALUES ({placeholders})
                """,
                params,
            )

        conn.commit()
        print(
            f"OK: range_scan_control criado. "
            f"data_inicio={data_inicio} data_fim={data_fim} "
            f"id_inicial={id_inicial} id_final={id_final}"
        )

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
