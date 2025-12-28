# detail.py  (ou sync_resolved_detail.py)
# Sync de tickets Resolved/Closed/Canceled + tratamento de "Ticket muito grande"
# Compatível com o seu schema atual onde:
#   audit_recent_run tem PK (id)
#   audit_recent_missing.run_id é FK -> audit_recent_run(id)

import os
import sys
import time
import logging
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg2
from psycopg2.extras import Json


API_BASE = "https://api.movidesk.com/public/v1"

BASE_STATUSES = ("Resolved", "Closed", "Canceled")

SELECT_LIST = "id,baseStatus,lastUpdate,isDeleted,actionCount"
SELECT_META = "id,baseStatus,lastUpdate,isDeleted,actionCount"
SELECT_DETAIL = (
    "id,protocol,type,subject,category,urgency,status,baseStatus,justification,origin,"
    "createdDate,isDeleted,owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
    "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,resolvedIn,closedIn,"
    "canceledIn,actionCount,reopenedIn,lastActionDate,lastUpdate,clients,statusHistories,"
    "customFieldValues"
)


# -------------------------
# helpers
# -------------------------
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def parse_intlike(v):
    """Converte int/float/str numérica para int, senão None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        try:
            return int(v)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            # aceita "2747" e "2747.0"
            if "." in s:
                return int(float(s))
            return int(s)
        except Exception:
            return None
    return None


def mask_token(url: str) -> str:
    try:
        p = urlparse(url)
        qs = parse_qsl(p.query, keep_blank_values=True)
        qs2 = []
        for k, v in qs:
            if k.lower() == "token":
                qs2.append((k, "***"))
            else:
                qs2.append((k, v))
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs2, doseq=True), p.fragment))
    except Exception:
        return url


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("detail")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger


# -------------------------
# DB
# -------------------------
def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "15")),
        application_name=os.getenv("PGAPPNAME", "movidesk-detail"),
        keepalives=1,
        keepalives_idle=int(os.getenv("PGKEEPALIVES_IDLE", "30")),
        keepalives_interval=int(os.getenv("PGKEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PGKEEPALIVES_COUNT", "5")),
    )


def db_run(dsn: str, fn):
    for i in (1, 2):
        conn = None
        try:
            conn = pg_connect(dsn)
            conn.autocommit = False
            res = fn(conn)
            conn.commit()
            return res
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if i == 1:
                continue
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def db_try(logger: logging.Logger, dsn: str, fn, label: str) -> bool:
    try:
        db_run(dsn, fn)
        return True
    except Exception as e:
        logger.info(f"detail: DB falhou em {label}: {type(e).__name__}: {e}")
        return False


def pg_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchone()


def pg_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()


def get_table_columns(conn, schema: str, table: str):
    rows = pg_all(
        conn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        """,
        [schema, table],
    )
    return {r[0] for r in rows}


def get_columns_info(conn, schema: str, table: str):
    rows = pg_all(
        conn,
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    info = {}
    for name, dtype, is_nullable, coldef in rows:
        info[name] = {"data_type": (dtype or "").lower(), "nullable": (is_nullable == "YES"), "default": coldef}
    return info


def get_column_type(conn, schema: str, table: str, column: str):
    row = pg_one(
        conn,
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """,
        [schema, table, column],
    )
    return row[0] if row else None


def get_primary_key_columns(conn, schema: str, table: str):
    rowset = pg_all(
        conn,
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = %s
          AND c.relname = %s
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        [schema, table],
    )
    return [r[0] for r in rowset]


def constraint_exists(conn, schema: str, table: str, conname: str) -> bool:
    row = pg_one(
        conn,
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname=%s AND t.relname=%s AND c.conname=%s
        """,
        [schema, table, conname],
    )
    return bool(row)


# -------------------------
# HTTP
# -------------------------
def request_with_retry(
    session: requests.Session,
    logger: logging.Logger,
    method: str,
    url: str,
    params: dict,
    timeout: tuple,
    attempts: int,
):
    last_exc = None
    last_resp = None
    for i in range(1, attempts + 1):
        try:
            req = requests.Request(method=method, url=url, params=params)
            prepped = session.prepare_request(req)
            masked = mask_token(prepped.url)
            resp = session.send(prepped, timeout=timeout)
            last_resp = resp
            logger.info(f"GET {masked} -> {resp.status_code} (attempt {i}/{attempts})")

            # retry em status transitórios
            if resp.status_code in (408, 429, 500, 502, 503, 504):
                if i == attempts:
                    return resp
                time.sleep(min(30, 2 ** (i - 1)))
                continue

            return resp
        except Exception as e:
            last_exc = e
            logger.info(f"GET {mask_token(url)} -> EXC (attempt {i}/{attempts}): {type(e).__name__}: {e}")
            if i == attempts:
                break
            time.sleep(min(30, 2 ** (i - 1)))
            continue

    if last_resp is not None:
        return last_resp
    raise last_exc if last_exc else RuntimeError("request failed")


def list_tickets(
    session: requests.Session,
    logger: logging.Logger,
    token: str,
    flt: str,
    select: str,
    timeout: tuple,
    attempts: int,
    top: int | None = None,
    orderby: str | None = None,
):
    params = {"$filter": flt, "$select": select, "includeDeletedItems": "true", "token": token}
    if top is not None:
        params["$top"] = str(top)
    if orderby:
        params["$orderby"] = orderby
    try:
        resp = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets", params, timeout, attempts)
    except Exception as exc:
        return [], None, f"{type(exc).__name__}: {exc}"
    if resp is None:
        return [], None, "request failed"
    if resp.status_code != 200:
        try:
            body = resp.text
        except Exception:
            body = ""
        return [], resp.status_code, body
    try:
        data = resp.json()
    except Exception:
        return [], resp.status_code, resp.text
    if isinstance(data, list):
        return data, 200, None
    return [], 200, None


def get_ticket_meta(
    session: requests.Session,
    logger: logging.Logger,
    token: str,
    ticket_id: int,
    timeout: tuple,
    attempts: int,
):
    flt = f"id eq {int(ticket_id)}"
    params = {"$filter": flt, "$select": SELECT_META, "includeDeletedItems": "true", "token": token, "$top": "1"}
    try:
        resp = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets", params, timeout, attempts)
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"
    if resp is None:
        return None, None, "request failed"
    if resp.status_code == 200:
        try:
            data = resp.json()
        except Exception:
            return None, 200, resp.text
        if isinstance(data, list) and data:
            return data[0], 200, None
        return None, 200, None
    try:
        return None, resp.status_code, resp.text
    except Exception:
        return None, resp.status_code, None


  # noqa: E501


def get_ticket_detail(
    session: requests.Session,
    logger: logging.Logger,
    token: str,
    ticket_id: int,
    timeout: tuple,
    attempts: int,
):
    params = {"id": str(ticket_id), "$select": SELECT_DETAIL, "includeDeletedItems": "true", "token": token}
    try:
        resp = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets", params, timeout, attempts)
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"

    if resp is None:
        return None, None, "request failed"

    if resp.status_code == 200:
        try:
            return resp.json(), 200, None
        except Exception:
            return None, 200, resp.text

    if resp.status_code != 404:
        try:
            return None, resp.status_code, resp.text
        except Exception:
            return None, resp.status_code, None

    # fallback: /tickets/past
    params2 = {"$filter": f"id eq {int(ticket_id)}", "$select": SELECT_DETAIL, "includeDeletedItems": "true", "token": token}
    try:
        resp2 = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets/past", params2, timeout, attempts)
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"

    if resp2 is None:
        return None, None, "request failed"

    if resp2.status_code != 200:
        try:
            return None, resp2.status_code, resp2.text
        except Exception:
            return None, resp2.status_code, None

    try:
        data = resp2.json()
    except Exception:
        return None, 200, resp2.text

    if isinstance(data, list) and data:
        return data[0], 200, None

    return None, 404, None


def is_timeout_like(status_code: int | None, err: str | None):
    if status_code in (408, 429, 500, 502, 503, 504):
        return True
    if not err:
        return False
    e = err.lower()
    return ("readtimeout" in e) or ("timed out" in e) or ("timeout" in e)


# -------------------------
# AUDIT tables (compatível com seu schema atual)
# -------------------------
def ensure_audit_run_table(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_run
            (
              id bigserial PRIMARY KEY,
              created_at timestamptz NOT NULL DEFAULT now(),
              external_run_id bigint,
              table_name text NOT NULL DEFAULT 'tickets_resolvidos_detail',
              workflow text,
              job text,
              ref text,
              sha text,
              repo text
            )
            """
        )
        # garante colunas (não mexe no PK existente)
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS id bigserial")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS created_at timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS external_run_id bigint")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS table_name text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS workflow text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS job text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS ref text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS sha text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_run ADD COLUMN IF NOT EXISTS repo text")
    return f"{schema}.audit_recent_run"


def ensure_audit_missing_table(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_missing
            (
              ticket_id bigint PRIMARY KEY,
              run_id bigint,
              table_name text NOT NULL DEFAULT 'tickets_resolvidos_detail',
              run_started_at timestamptz NOT NULL DEFAULT now(),
              first_seen timestamptz NOT NULL DEFAULT now(),
              last_seen timestamptz NOT NULL DEFAULT now(),
              attempts integer NOT NULL DEFAULT 0,
              last_attempt timestamptz,
              last_status integer,
              last_error text
            )
            """
        )
        # garante colunas
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS ticket_id bigint")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS run_id bigint")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS table_name text")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS run_started_at timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS first_seen timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_seen timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS attempts integer")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_attempt timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_status integer")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_error text")

        # garante defaults básicos para evitar NOT NULL / nulos antigos
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET attempts=0 WHERE attempts IS NULL")
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET first_seen=now() WHERE first_seen IS NULL")
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET last_seen=now() WHERE last_seen IS NULL")

        # tenta garantir FK (run_id -> audit_recent_run.id) se ainda não existir
        fk_name = "audit_recent_missing_run_id_fkey"
        if not constraint_exists(conn, schema, "audit_recent_missing", fk_name):
            try:
                cur.execute(
                    f"""
                    ALTER TABLE {schema}.audit_recent_missing
                    ADD CONSTRAINT {fk_name}
                    FOREIGN KEY (run_id)
                    REFERENCES {schema}.audit_recent_run(id)
                    ON DELETE CASCADE
                    """
                )
            except Exception:
                # se falhar (já existe diferente / dados inválidos), não interrompe
                pass

    return f"{schema}.audit_recent_missing"


def ensure_audit_tables(conn, logger: logging.Logger):
    schema = os.getenv("AUDIT_SCHEMA", "visualizacao_resolvidos")
    run_fqn = ensure_audit_run_table(conn, schema)
    miss_fqn = ensure_audit_missing_table(conn, schema)
    logger.info(f"detail: audit_recent_missing em {miss_fqn}")
    return run_fqn, miss_fqn


def run_context():
    external_run_id = _env_int("GITHUB_RUN_ID", _env_int("RUN_ID", 0))
    if external_run_id <= 0:
        external_run_id = int(time.time())

    table_name = os.getenv("DETAIL_TABLE_NAME", "tickets_resolvidos_detail")

    return {
        "external_run_id": external_run_id,
        "table_name": table_name,
        "workflow": os.getenv("GITHUB_WORKFLOW", "") or "",
        "job": os.getenv("GITHUB_JOB", "") or "",
        "ref": os.getenv("GITHUB_REF", "") or "",
        "sha": os.getenv("GITHUB_SHA", "") or "",
        "repo": os.getenv("GITHUB_REPOSITORY", "") or "",
        "run_db_id": None,  # preenchido no começo do main
    }


def audit_run_create_and_get_id(conn, run_fqn: str, ctx: dict) -> int | None:
    """
    Cria uma linha em audit_recent_run e retorna o id (PK) para usar como FK em audit_recent_missing.run_id.
    Compatível com seu schema atual: PK (id).
    """
    schema, table = run_fqn.split(".", 1)
    cols = get_table_columns(conn, schema, table)
    pk = get_primary_key_columns(conn, schema, table)

    ext = int(ctx["external_run_id"])

    # caso raro: se PK for run_id
    if pk == ["run_id"] and "run_id" in cols:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (run_id, table_name, created_at)
                VALUES (%s, %s, now())
                ON CONFLICT (run_id) DO NOTHING
                """,
                [ext, ctx.get("table_name", "tickets_resolvidos_detail")],
            )
        return ext

    # padrão: PK (id)
    if "id" not in cols:
        return None

    # se existir coluna external_run_id, tenta reutilizar (evita duplicar)
    if "external_run_id" in cols:
        row = pg_one(conn, f"SELECT id FROM {schema}.{table} WHERE external_run_id=%s ORDER BY id DESC LIMIT 1", [ext])
        if row:
            return int(row[0])

    insert_cols = []
    insert_vals = []
    params = []

    def add(col: str, val):
        insert_cols.append(col)
        insert_vals.append("%s")
        params.append(val)

    def add_now(col: str):
        insert_cols.append(col)
        insert_vals.append("now()")

    if "created_at" in cols:
        add_now("created_at")
    if "external_run_id" in cols:
        add("external_run_id", ext)
    if "table_name" in cols:
        add("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))
    for c in ("workflow", "job", "ref", "sha", "repo"):
        if c in cols:
            add(c, ctx.get(c, ""))

    if not insert_cols:
        # mínimo: insere default
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {schema}.{table} DEFAULT VALUES RETURNING id")
            rid = cur.fetchone()[0]
        return int(rid)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {schema}.{table} ({','.join(insert_cols)}) VALUES ({','.join(insert_vals)}) RETURNING id",
            params,
        )
        rid = cur.fetchone()[0]
    return int(rid)


# -------------------------
# DETAIL target table
# -------------------------
def get_detail_table_info(conn):
    schema = "visualizacao_resolvidos"
    table = "tickets_resolvidos_detail"
    cols = get_table_columns(conn, schema, table)

    raw_col = None
    for c in ("raw", "raw_json", "payload", "data"):
        if c in cols:
            raw_col = c
            break
    if raw_col is None:
        raise RuntimeError(f"Nenhuma coluna de JSON encontrada em {schema}.{table} (raw/raw_json/payload/data)")

    dt = (get_column_type(conn, schema, table, raw_col) or "").lower()
    raw_is_json = dt in ("json", "jsonb")
    return cols, raw_col, raw_is_json


def upsert_detail(conn, ticket: dict, cols: set[str], raw_col: str, raw_is_json: bool):
    schema = "visualizacao_resolvidos"
    table = "tickets_resolvidos_detail"

    insert_cols = ["ticket_id", raw_col]
    placeholders = ["%s", "%s"]

    raw_val = Json(ticket) if raw_is_json else str(ticket)
    values = [int(ticket.get("id")), raw_val]

    base_status = ticket.get("baseStatus")
    last_update = ticket.get("lastUpdate")

    if "base_status" in cols:
        insert_cols.append("base_status")
        placeholders.append("%s")
        values.append(base_status)
    if "last_update" in cols:
        insert_cols.append("last_update")
        placeholders.append("%s")
        values.append(last_update)
    if "updated_at" in cols:
        insert_cols.append("updated_at")
        placeholders.append("now()")

    update_sets = [f"{raw_col}=EXCLUDED.{raw_col}"]
    if "base_status" in cols:
        update_sets.append("base_status=EXCLUDED.base_status")
    if "last_update" in cols:
        update_sets.append("last_update=EXCLUDED.last_update")
    if "updated_at" in cols:
        update_sets.append("updated_at=now()")

    sql = f"""
    INSERT INTO {schema}.{table} ({",".join(insert_cols)})
    VALUES ({",".join(placeholders)})
    ON CONFLICT (ticket_id) DO UPDATE SET {",".join(update_sets)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)


def upsert_too_big(conn, ticket_id: int, base_status, last_update, cols: set[str], raw_col: str, raw_is_json: bool):
    schema = "visualizacao_resolvidos"
    table = "tickets_resolvidos_detail"

    insert_cols = ["ticket_id", raw_col]
    placeholders = ["%s", "%s"]

    raw_val = Json("Ticket muito grande") if raw_is_json else "Ticket muito grande"
    values = [int(ticket_id), raw_val]

    if "base_status" in cols:
        insert_cols.append("base_status")
        placeholders.append("%s")
        values.append(base_status)
    if "last_update" in cols:
        insert_cols.append("last_update")
        placeholders.append("%s")
        values.append(last_update)
    if "updated_at" in cols:
        insert_cols.append("updated_at")
        placeholders.append("now()")

    update_sets = [f"{raw_col}=EXCLUDED.{raw_col}"]
    if "base_status" in cols:
        update_sets.append("base_status=EXCLUDED.base_status")
    if "last_update" in cols:
        update_sets.append("last_update=EXCLUDED.last_update")
    if "updated_at" in cols:
        update_sets.append("updated_at=now()")

    sql = f"""
    INSERT INTO {schema}.{table} ({",".join(insert_cols)})
    VALUES ({",".join(placeholders)})
    ON CONFLICT (ticket_id) DO UPDATE SET {",".join(update_sets)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)


# -------------------------
# audit queue ops
# -------------------------
def audit_upsert(conn, audit_fqn: str, ctx: dict, ticket_id: int, status_code: int | None, error_text: str | None):
    schema, table = audit_fqn.split(".", 1)
    info = get_columns_info(conn, schema, table)
    cols = set(info.keys())

    insert_cols = []
    insert_vals = []
    params = []

    def add(col: str, val):
        insert_cols.append(col)
        insert_vals.append("%s")
        params.append(val)

    def add_now(col: str):
        insert_cols.append(col)
        insert_vals.append("now()")

    if "ticket_id" in cols:
        add("ticket_id", int(ticket_id))

    # FK -> audit_recent_run(id)
    run_db_id = ctx.get("run_db_id")
    if "run_id" in cols and run_db_id is not None:
        add("run_id", int(run_db_id))

    if "table_name" in cols:
        add("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))

    if "run_started_at" in cols:
        add_now("run_started_at")

    if "first_seen" in cols:
        add_now("first_seen")
    if "last_seen" in cols:
        add_now("last_seen")
    if "attempts" in cols:
        add("attempts", 1)
    if "last_attempt" in cols:
        add_now("last_attempt")
    if "last_status" in cols:
        add("last_status", status_code)
    if "last_error" in cols:
        add("last_error", error_text)

    # preenche qualquer NOT NULL sem default
    for col, meta in info.items():
        if col in insert_cols:
            continue
        if meta["nullable"]:
            continue
        if meta["default"] is not None:
            continue
        dt = meta["data_type"]
        if "timestamp" in dt or dt == "date":
            add_now(col)
            continue
        if dt in ("integer", "bigint", "smallint", "numeric", "real", "double precision"):
            add(col, 0)
            continue
        if dt == "boolean":
            add(col, False)
            continue
        if dt in ("json", "jsonb"):
            add(col, Json({}))
            continue
        add(col, "")

    updates = []
    if "last_seen" in cols:
        updates.append("last_seen=now()")
    if "attempts" in cols:
        updates.append(f"attempts=COALESCE({schema}.{table}.attempts,0) + 1")
    if "last_attempt" in cols:
        updates.append("last_attempt=now()")
    if "last_status" in cols:
        updates.append("last_status=EXCLUDED.last_status")
    if "last_error" in cols:
        updates.append("last_error=EXCLUDED.last_error")
    if "run_id" in cols and run_db_id is not None:
        updates.append("run_id=EXCLUDED.run_id")
    if "table_name" in cols:
        updates.append("table_name=EXCLUDED.table_name")

    # conflito: normalmente PK(ticket_id)
    pk = get_primary_key_columns(conn, schema, table)
    conflict_cols = pk if pk else (["ticket_id"] if "ticket_id" in cols else [])

    if conflict_cols:
        conflict = ",".join(conflict_cols)
        sql = f"""
        INSERT INTO {schema}.{table} ({",".join(insert_cols)})
        VALUES ({",".join(insert_vals)})
        ON CONFLICT ({conflict}) DO UPDATE SET
          {",".join(updates) if updates else "ticket_id=EXCLUDED.ticket_id"}
        """
    else:
        sql = f"""
        INSERT INTO {schema}.{table} ({",".join(insert_cols)})
        VALUES ({",".join(insert_vals)})
        """

    with conn.cursor() as cur:
        cur.execute(sql, params)


def audit_delete(conn, audit_fqn: str, ticket_id: int):
    schema, table = audit_fqn.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{table} WHERE ticket_id=%s", [int(ticket_id)])


# -------------------------
# main
# -------------------------
def main():
    logger = setup_logger()

    dsn = os.getenv("NEON_DSN")
    token = os.getenv("MOVIDESK_TOKEN")
    if not dsn or not token:
        raise SystemExit("NEON_DSN e MOVIDESK_TOKEN são obrigatórios")

    bulk_limit = _env_int("DETAIL_BULK_LIMIT", 200)
    missing_limit = _env_int("DETAIL_MISSING_LIMIT", 10)
    window = _env_int("DETAIL_WINDOW", 50)

    attempts = _env_int("DETAIL_ATTEMPTS", 6)
    connect_timeout = _env_int("DETAIL_CONNECT_TIMEOUT", 10)
    read_timeout = _env_int("DETAIL_READ_TIMEOUT", 120)
    timeout = (connect_timeout, read_timeout)

    pending_attempts = _env_int("DETAIL_PENDING_ATTEMPTS", 2)
    pending_read_timeout = _env_int("DETAIL_PENDING_READ_TIMEOUT", 45)
    pending_timeout = (connect_timeout, pending_read_timeout)

    meta_attempts = _env_int("DETAIL_META_ATTEMPTS", 2)
    meta_read_timeout = _env_int("DETAIL_META_READ_TIMEOUT", 20)
    meta_timeout = (connect_timeout, meta_read_timeout)

    # >>> aqui: default 150 <<<
    max_actions = _env_int("DETAIL_MAX_ACTIONS", 150)

    # após quantas tentativas com timeout “desiste” e marca como Ticket muito grande
    big_after_attempts = _env_int("DETAIL_BIG_AFTER_ATTEMPTS", 2)

    ctx = run_context()

    logger.info(
        f"detail: Iniciando sync (bulk={bulk_limit}, missing={missing_limit}, window={window}, "
        f"timeout=({connect_timeout}s,{read_timeout}s), attempts={attempts}, max_actions={max_actions})."
    )

    def load_state(conn):
        run_fqn, audit_fqn = ensure_audit_tables(conn, logger)
        detail_cols, raw_col, raw_is_json = get_detail_table_info(conn)

        last_row = pg_one(conn, "SELECT COALESCE(MAX(ticket_id),0) FROM visualizacao_resolvidos.tickets_resolvidos_detail")
        last_id = int(last_row[0] if last_row else 0)

        lower = max(0, last_id - window - 1)
        upper = last_id + window

        db_rows = pg_all(
            conn,
            """
            SELECT ticket_id
            FROM visualizacao_resolvidos.tickets_resolvidos_detail
            WHERE ticket_id > %s AND ticket_id <= %s
            """,
            [lower, upper],
        )
        db_ids = {int(r[0]) for r in db_rows}

        schema, table = audit_fqn.split(".", 1)
        pend_rows = pg_all(
            conn,
            f"""
            SELECT ticket_id,
                   COALESCE(attempts,0) AS attempts,
                   COALESCE(last_error,'') AS last_error
            FROM {schema}.{table}
            ORDER BY ticket_id DESC
            LIMIT %s
            """,
            [missing_limit],
        )

        return run_fqn, audit_fqn, detail_cols, raw_col, raw_is_json, last_id, lower, upper, db_ids, pend_rows

    run_fqn, audit_fqn, detail_cols, raw_col, raw_is_json, last_id, lower, upper, db_ids, pend_rows = db_run(dsn, load_state)
    logger.info(f"detail: Último ticket_id em tickets_resolvidos_detail: {last_id}")

    # cria/pega o id do run (PK do audit_recent_run) para resolver FK do missing
    ctx["run_db_id"] = db_run(dsn, lambda conn: audit_run_create_and_get_id(conn, run_fqn, ctx))

    session = requests.Session()

    total_ok = 0
    total_new = 0
    total_window = 0
    total_missing = 0
    total_big = 0

    # -------------------------
    # Janela
    # -------------------------
    flt_window = (
        f"id gt {lower} and id le {upper} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )

    logger.info(f"detail: Janela: /tickets com id > {lower} e <= {upper} (Resolved/Closed/Canceled)")
    window_items, _, _ = list_tickets(session, logger, token, flt_window, SELECT_LIST, timeout, attempts, top=1000, orderby="id")

    meta_map = {}
    api_ids = set()
    for x in window_items:
        if isinstance(x, dict) and x.get("id") is not None:
            tid = parse_intlike(x.get("id"))
            if tid is None:
                continue
            api_ids.add(tid)
            meta_map[tid] = x

    to_fetch = sorted(list(api_ids - db_ids))
    logger.info(f"detail: Janela (center={last_id}): encontrados={len(api_ids)}, já_existiam={len(db_ids)}, para_buscar={len(to_fetch)}")

    for idx, tid in enumerate(to_fetch[: max(0, bulk_limit)], start=1):
        m = meta_map.get(tid) or {}
        ac = parse_intlike(m.get("actionCount"))
        bs = m.get("baseStatus")
        lu = m.get("lastUpdate")

        if ac is not None and ac > max_actions:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(window)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window)")
            total_big += 1
            total_window += 1
            continue

        logger.info(f"detail: Janela: buscando detalhe {idx}/{min(len(to_fetch), bulk_limit)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            ok = db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(window)")
            if ok:
                total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(window)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window)")
        total_ok += 1
        total_window += 1

    # -------------------------
    # Novos
    # -------------------------
    flt_new = (
        f"id gt {last_id} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )
    new_items, _, _ = list_tickets(session, logger, token, flt_new, SELECT_LIST, timeout, attempts, top=bulk_limit, orderby="id")

    new_ids = []
    new_meta = {}
    for x in new_items:
        if isinstance(x, dict) and x.get("id") is not None:
            tid = parse_intlike(x.get("id"))
            if tid is None:
                continue
            new_ids.append(tid)
            new_meta[tid] = x

    logger.info(f"detail: {len(new_ids)} tickets em /tickets com id > {last_id} (Resolved/Closed/Canceled)")
    for idx, tid in enumerate(new_ids, start=1):
        m = new_meta.get(tid) or {}
        ac = parse_intlike(m.get("actionCount"))
        bs = m.get("baseStatus")
        lu = m.get("lastUpdate")

        if ac is not None and ac > max_actions:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(new)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new)")
            total_big += 1
            total_new += 1
            continue

        logger.info(f"detail: Novo: buscando detalhe {idx}/{len(new_ids)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            ok = db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(new)")
            if ok:
                total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(new)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new)")
        total_ok += 1
        total_new += 1

    # -------------------------
    # Pendentes (audit_recent_missing)
    # -------------------------
    pend_ids = [(parse_intlike(r[0]), int(r[1] or 0), str(r[2] or "")) for r in pend_rows]
    pend_ids = [(tid, a, e) for (tid, a, e) in pend_ids if tid is not None]

    logger.info(f"detail: {len(pend_ids)} tickets pendentes na fila audit_recent_missing (limite={missing_limit}).")

    for idx, (tid, prev_attempts, prev_err) in enumerate(pend_ids, start=1):
        logger.info(f"detail: Processando ticket pendente {idx}/{len(pend_ids)} (ID={tid})")

        # se já tentou várias vezes e só dá timeout, marca como grande e remove da fila
        if prev_attempts >= big_after_attempts and is_timeout_like(None, prev_err):
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_by_attempts)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_by_attempts)")
            total_big += 1
            continue

        # tenta meta primeiro (mais leve) para pegar actionCount/baseStatus/lastUpdate
        meta_obj, msc, me = get_ticket_meta(session, logger, token, tid, meta_timeout, meta_attempts)
        if isinstance(meta_obj, dict):
            ac = parse_intlike(meta_obj.get("actionCount"))
            bs = meta_obj.get("baseStatus")
            lu = meta_obj.get("lastUpdate")
            if ac is not None and ac > max_actions:
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_meta)")
                total_big += 1
                continue

        # se até o meta dá timeout e já passou do limite, desiste e marca grande
        if meta_obj is None and is_timeout_like(msc, me) and (prev_attempts + 1) >= big_after_attempts:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta_timeout)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_meta_timeout)")
            total_big += 1
            continue

        ticket, sc, e = get_ticket_detail(session, logger, token, tid, pending_timeout, pending_attempts)
        if ticket is None:
            # se detail deu timeout e já está no limite de tentativas, marca grande
            if is_timeout_like(sc, e) and (prev_attempts + 1) >= big_after_attempts:
                bs = meta_obj.get("baseStatus") if isinstance(meta_obj, dict) else None
                lu = meta_obj.get("lastUpdate") if isinstance(meta_obj, dict) else None
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_detail_timeout)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_detail_timeout)")
                total_big += 1
                continue

            ok = db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(pending)")
            if ok:
                total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(pending)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending)")
        total_ok += 1

    logger.info(
        f"detail: Finalizado. OK={total_ok} (janela={total_window}, novos={total_new}), "
        f"Muito_grandes={total_big}, Falhas={total_missing}."
    )


if __name__ == "__main__":
    main()
