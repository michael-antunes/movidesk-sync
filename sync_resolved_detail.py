# sync_resolved_detail.py
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

# inclui actionCount pra gente identificar ticket "gigante" antes de buscar o detail
SELECT_LIST = "id,baseStatus,lastUpdate,isDeleted,actionCount"
SELECT_META = "id,baseStatus,lastUpdate,isDeleted,actionCount"

# detalhe completo (pode ser pesado em tickets com muitas ações)
SELECT_DETAIL = (
    "id,protocol,type,subject,category,urgency,status,baseStatus,justification,origin,"
    "createdDate,isDeleted,owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
    "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,resolvedIn,closedIn,"
    "canceledIn,actionCount,reopenedIn,lastActionDate,lastUpdate,clients,statusHistories,"
    "customFieldValues"
)


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


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


# -----------------------
# DB helpers
# -----------------------
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
            try:
                conn.commit()
            except Exception:
                pass
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


def get_column_type(conn, schema: str, table: str, column: str) -> str | None:
    row = pg_one(
        conn,
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """,
        [schema, table, column],
    )
    return (row[0] if row else None)


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


# -----------------------
# HTTP helpers
# -----------------------
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

    # fallback: tickets/past
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


# -----------------------
# Detail table helpers
# -----------------------
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


def upsert_too_big(
    conn,
    ticket_id: int,
    base_status: str | None,
    last_update: str | None,
    cols: set[str],
    raw_col: str,
    raw_is_json: bool,
):
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


# -----------------------
# Audit tables (ensure + write)
# -----------------------
def ensure_audit_tables(conn, logger: logging.Logger):
    schema = os.getenv("AUDIT_SCHEMA", "visualizacao_resolvidos")

    # Cria estruturas "mínimas" caso não existam. Se já existem com mais colunas/constraints, não atrapalha.
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # audit_recent_run (PK normalmente é id)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_run
            (
              id bigserial PRIMARY KEY,
              created_at timestamptz NOT NULL DEFAULT now(),
              window_start bigint NOT NULL DEFAULT 0,
              window_end bigint NOT NULL DEFAULT 0,
              table_name text NOT NULL DEFAULT 'tickets_resolvidos_detail',
              run_id bigint,
              workflow text,
              job text,
              ref text,
              sha text,
              repo text,
              max_actions integer
            )
            """
        )

        # adiciona colunas caso faltem (sem mexer em NOT NULL dos existentes)
        for col_ddl in (
            "ADD COLUMN IF NOT EXISTS created_at timestamptz",
            "ADD COLUMN IF NOT EXISTS window_start bigint",
            "ADD COLUMN IF NOT EXISTS window_end bigint",
            "ADD COLUMN IF NOT EXISTS table_name text",
            "ADD COLUMN IF NOT EXISTS run_id bigint",
            "ADD COLUMN IF NOT EXISTS workflow text",
            "ADD COLUMN IF NOT EXISTS job text",
            "ADD COLUMN IF NOT EXISTS ref text",
            "ADD COLUMN IF NOT EXISTS sha text",
            "ADD COLUMN IF NOT EXISTS repo text",
            "ADD COLUMN IF NOT EXISTS max_actions integer",
        ):
            cur.execute(f"ALTER TABLE {schema}.audit_recent_run {col_ddl}")

        # audit_recent_missing (muitas vezes ticket_id é PK, mas seu schema pode não ter. A gente lida no upsert.)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_missing
            (
              ticket_id bigint PRIMARY KEY,
              run_id bigint,
              table_name text,
              run_started_at timestamptz,
              first_seen timestamptz NOT NULL DEFAULT now(),
              last_seen timestamptz NOT NULL DEFAULT now(),
              attempts integer NOT NULL DEFAULT 0,
              last_attempt timestamptz,
              last_status integer,
              last_error text
            )
            """
        )
        for col_ddl in (
            "ADD COLUMN IF NOT EXISTS ticket_id bigint",
            "ADD COLUMN IF NOT EXISTS run_id bigint",
            "ADD COLUMN IF NOT EXISTS table_name text",
            "ADD COLUMN IF NOT EXISTS run_started_at timestamptz",
            "ADD COLUMN IF NOT EXISTS first_seen timestamptz",
            "ADD COLUMN IF NOT EXISTS last_seen timestamptz",
            "ADD COLUMN IF NOT EXISTS attempts integer",
            "ADD COLUMN IF NOT EXISTS last_attempt timestamptz",
            "ADD COLUMN IF NOT EXISTS last_status integer",
            "ADD COLUMN IF NOT EXISTS last_error text",
        ):
            cur.execute(f"ALTER TABLE {schema}.audit_recent_missing {col_ddl}")

    logger.info(f"detail: audit_recent_missing em {schema}.audit_recent_missing")
    return f"{schema}.audit_recent_run", f"{schema}.audit_recent_missing"


def run_context(max_actions: int):
    run_id = _env_int("GITHUB_RUN_ID", _env_int("RUN_ID", 0))
    if run_id <= 0:
        run_id = int(time.time())
    return {
        "external_run_id": run_id,  # GitHub run id
        "table_name": "tickets_resolvidos_detail",
        "workflow": os.getenv("GITHUB_WORKFLOW", "") or "",
        "job": os.getenv("GITHUB_JOB", "") or "",
        "ref": os.getenv("GITHUB_REF", "") or "",
        "sha": os.getenv("GITHUB_SHA", "") or "",
        "repo": os.getenv("GITHUB_REPOSITORY", "") or "",
        "max_actions": max_actions,
        "window_start": 0,
        "window_end": 0,
        "window_center": 0,
    }


def audit_run_create_and_get_id(conn, run_fqn: str, ctx: dict) -> int | None:
    """
    Cria um registro em audit_recent_run e retorna o PK (id) para usar como FK em audit_recent_missing.run_id.
    Robusto: se o run table tiver colunas NOT NULL sem default (ex: window_start), a função preenche.
    """
    schema, table = run_fqn.split(".", 1)
    info = get_columns_info(conn, schema, table)
    cols = set(info.keys())

    ext = int(ctx.get("external_run_id") or 0)

    # se existir coluna "run_id" (comum no seu schema), tenta reaproveitar o mesmo run
    if "run_id" in cols and ext > 0:
        row = pg_one(conn, f"SELECT id FROM {schema}.{table} WHERE run_id=%s ORDER BY id DESC LIMIT 1", [ext])
        if row:
            return int(row[0])

    insert_cols = []
    insert_vals = []
    params = []

    def add_param(col: str, val):
        insert_cols.append(col)
        insert_vals.append("%s")
        params.append(val)

    def add_now(col: str):
        insert_cols.append(col)
        insert_vals.append("now()")

    # campos conhecidos
    if "created_at" in cols:
        add_now("created_at")
    if "run_id" in cols and ext > 0:
        add_param("run_id", ext)
    if "table_name" in cols:
        add_param("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))
    if "workflow" in cols:
        add_param("workflow", ctx.get("workflow", ""))
    if "job" in cols:
        add_param("job", ctx.get("job", ""))
    if "ref" in cols:
        add_param("ref", ctx.get("ref", ""))
    if "sha" in cols:
        add_param("sha", ctx.get("sha", ""))
    if "repo" in cols:
        add_param("repo", ctx.get("repo", ""))
    if "max_actions" in cols:
        add_param("max_actions", int(ctx.get("max_actions") or 0))

    # IMPORTANTÍSSIMO: window_start/window_end (no seu schema podem ser NOT NULL)
    if "window_start" in cols:
        add_param("window_start", int(ctx.get("window_start") or 0))
    if "window_end" in cols:
        add_param("window_end", int(ctx.get("window_end") or 0))

    # alguns schemas têm started_at/run_started_at
    for c in ("started_at", "run_started_at"):
        if c in cols and c not in insert_cols:
            add_now(c)

    # preenche qualquer NOT NULL sem default que ainda não entrou no insert
    for col, meta in info.items():
        if col in insert_cols:
            continue
        if meta["nullable"]:
            continue
        if meta["default"] is not None:
            continue
        if col in ("id",):
            continue

        dt = (meta["data_type"] or "").lower()
        if "timestamp" in dt or dt == "date":
            add_now(col)
        elif dt in ("integer", "bigint", "smallint", "numeric", "real", "double precision"):
            add_param(col, 0)
        elif dt == "boolean":
            add_param(col, False)
        elif dt in ("json", "jsonb"):
            add_param(col, Json({}))
        else:
            add_param(col, "")

    if not insert_cols:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {schema}.{table} DEFAULT VALUES RETURNING id")
            return int(cur.fetchone()[0])

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {schema}.{table} ({','.join(insert_cols)}) VALUES ({','.join(insert_vals)}) RETURNING id",
            params,
        )
        return int(cur.fetchone()[0])


def get_audit_missing_conflict_cols(conn, audit_fqn: str):
    schema, table = audit_fqn.split(".", 1)
    pk = get_primary_key_columns(conn, schema, table)
    if pk:
        return pk
    # tenta achar um UNIQUE também
    rows = pg_all(
        conn,
        """
        SELECT a.attname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        JOIN unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ord) ON TRUE
        JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = ck.attnum
        WHERE nsp.nspname = %s
          AND rel.relname = %s
          AND con.contype = 'u'
        ORDER BY con.conname, ck.ord
        """,
        [schema, table],
    )
    cols = [r[0] for r in rows]
    if cols:
        return cols
    # fallback: se tiver ticket_id, a gente faz delete+insert
    tcols = get_table_columns(conn, schema, table)
    if "ticket_id" in tcols:
        return []
    return []


def audit_upsert(conn, audit_fqn: str, ctx: dict, ticket_id: int, status_code: int | None, error_text: str | None):
    schema, table = audit_fqn.split(".", 1)
    info = get_columns_info(conn, schema, table)
    cols = set(info.keys())

    conflict_cols = get_audit_missing_conflict_cols(conn, audit_fqn)

    insert_cols = []
    insert_vals = []
    params = []

    def add_param(col: str, val):
        insert_cols.append(col)
        insert_vals.append("%s")
        params.append(val)

    def add_now(col: str):
        insert_cols.append(col)
        insert_vals.append("now()")

    # campos principais
    if "ticket_id" in cols:
        add_param("ticket_id", int(ticket_id))

    # FK run_id -> audit_recent_run(id)
    # ctx["run_db_id"] é o ID gerado no audit_recent_run
    if "run_id" in cols:
        add_param("run_id", int(ctx.get("run_db_id") or 0))

    if "table_name" in cols:
        add_param("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))

    if "run_started_at" in cols:
        add_now("run_started_at")

    if "first_seen" in cols:
        add_now("first_seen")
    if "last_seen" in cols:
        add_now("last_seen")
    if "attempts" in cols:
        add_param("attempts", 1)
    if "last_attempt" in cols:
        add_now("last_attempt")
    if "last_status" in cols:
        add_param("last_status", status_code)
    if "last_error" in cols:
        add_param("last_error", error_text)

    # completa NOT NULL sem default
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
        elif dt in ("integer", "bigint", "smallint", "numeric", "real", "double precision"):
            add_param(col, 0)
        elif dt == "boolean":
            add_param(col, False)
        elif dt in ("json", "jsonb"):
            add_param(col, Json({}))
        else:
            add_param(col, "")

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
    if "run_id" in cols:
        updates.append("run_id=EXCLUDED.run_id")
    if "table_name" in cols:
        updates.append("table_name=EXCLUDED.table_name")

    with conn.cursor() as cur:
        if conflict_cols:
            conflict = ",".join(conflict_cols)
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} ({",".join(insert_cols)})
                VALUES ({",".join(insert_vals)})
                ON CONFLICT ({conflict}) DO UPDATE SET {",".join(updates) if updates else "ticket_id=EXCLUDED.ticket_id"}
                """,
                params,
            )
        else:
            # sem PK/UNIQUE: garante 1 linha por ticket_id na marra
            if "ticket_id" in cols:
                cur.execute(f"DELETE FROM {schema}.{table} WHERE ticket_id=%s", [int(ticket_id)])
            cur.execute(
                f"INSERT INTO {schema}.{table} ({','.join(insert_cols)}) VALUES ({','.join(insert_vals)})",
                params,
            )


def audit_delete(conn, audit_fqn: str, ticket_id: int):
    schema, table = audit_fqn.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{table} WHERE ticket_id=%s", [int(ticket_id)])


# -----------------------
# main
# -----------------------
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

    # default agora é 150 (como você pediu)
    max_actions = _env_int("DETAIL_MAX_ACTIONS", 150)

    # após X tentativas com timeout, marca como "Ticket muito grande"
    big_after_attempts = _env_int("DETAIL_BIG_AFTER_ATTEMPTS", 2)

    ctx = run_context(max_actions=max_actions)

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

    # salva janela no ctx (pra não quebrar audit_recent_run.window_start NOT NULL)
    ctx["window_start"] = lower
    ctx["window_end"] = upper
    ctx["window_center"] = last_id

    logger.info(f"detail: Último ticket_id em tickets_resolvidos_detail: {last_id}")

    # cria run no banco e pega o ID (PK) pra usar como FK no missing
    ctx["run_db_id"] = db_run(dsn, lambda conn: audit_run_create_and_get_id(conn, run_fqn, ctx))

    session = requests.Session()

    total_ok = 0
    total_new = 0
    total_window = 0
    total_missing = 0
    total_big = 0

    # -----------------------
    # Window scan: pega lacunas no range [last-window, last+window]
    # -----------------------
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
            tid = int(x.get("id"))
            api_ids.add(tid)
            meta_map[tid] = x

    to_fetch = sorted(list(api_ids - db_ids))
    logger.info(f"detail: Janela (center={last_id}): encontrados={len(api_ids)}, já_existiam={len(db_ids)}, para_buscar={len(to_fetch)}")

    for idx, tid in enumerate(to_fetch[: max(0, bulk_limit)], start=1):
        m = meta_map.get(tid) or {}
        ac = m.get("actionCount")
        bs = m.get("baseStatus")
        lu = m.get("lastUpdate")

        if isinstance(ac, int) and ac > max_actions:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(window)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window)")
            total_big += 1
            total_window += 1
            continue

        logger.info(f"detail: Janela: buscando detalhe {idx}/{min(len(to_fetch), bulk_limit)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            db_try(
                logger,
                dsn,
                lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None),
                "audit_upsert(window)",
            )
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(window)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window)")
        total_ok += 1
        total_window += 1

    # -----------------------
    # New tickets: id > last_id
    # -----------------------
    flt_new = (
        f"id gt {last_id} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )
    new_items, _, _ = list_tickets(session, logger, token, flt_new, SELECT_LIST, timeout, attempts, top=bulk_limit, orderby="id")

    new_ids = []
    new_meta = {}
    for x in new_items:
        if isinstance(x, dict) and x.get("id") is not None:
            tid = int(x.get("id"))
            new_ids.append(tid)
            new_meta[tid] = x

    logger.info(f"detail: {len(new_ids)} tickets em /tickets com id > {last_id} (Resolved/Closed/Canceled)")

    for idx, tid in enumerate(new_ids, start=1):
        m = new_meta.get(tid) or {}
        ac = m.get("actionCount")
        bs = m.get("baseStatus")
        lu = m.get("lastUpdate")

        if isinstance(ac, int) and ac > max_actions:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(new)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new)")
            total_big += 1
            total_new += 1
            continue

        logger.info(f"detail: Novo: buscando detalhe {idx}/{len(new_ids)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            db_try(
                logger,
                dsn,
                lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None),
                "audit_upsert(new)",
            )
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(new)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new)")
        total_ok += 1
        total_new += 1

    # -----------------------
    # Pending from audit_recent_missing
    # -----------------------
    pend_ids = [(int(r[0]), int(r[1] or 0), str(r[2] or "")) for r in pend_rows]
    logger.info(f"detail: {len(pend_ids)} tickets pendentes na fila audit_recent_missing (limite={missing_limit}).")

    for idx, (tid, prev_attempts, prev_err) in enumerate(pend_ids, start=1):
        logger.info(f"detail: Processando ticket pendente {idx}/{len(pend_ids)} (ID={tid})")

        # se já tentou algumas vezes e só dá timeout, marca como "Ticket muito grande"
        if prev_attempts >= big_after_attempts and is_timeout_like(None, prev_err):
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_by_attempts)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_by_attempts)")
            total_big += 1
            continue

        # tenta meta rápido: se actionCount > max_actions, não busca detail
        meta_obj, msc, me = get_ticket_meta(session, logger, token, tid, meta_timeout, meta_attempts)
        if isinstance(meta_obj, dict):
            ac = meta_obj.get("actionCount")
            bs = meta_obj.get("baseStatus")
            lu = meta_obj.get("lastUpdate")
            if isinstance(ac, int) and ac > max_actions:
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_meta)")
                total_big += 1
                continue

        # se nem o meta responde (timeout) e já bateu o limite, marca too_big
        if meta_obj is None and is_timeout_like(msc, me) and (prev_attempts + 1) >= big_after_attempts:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta_timeout)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_meta_timeout)")
            total_big += 1
            continue

        ticket, sc, e = get_ticket_detail(session, logger, token, tid, pending_timeout, pending_attempts)
        if ticket is None:
            if is_timeout_like(sc, e) and (prev_attempts + 1) >= big_after_attempts:
                bs = meta_obj.get("baseStatus") if isinstance(meta_obj, dict) else None
                lu = meta_obj.get("lastUpdate") if isinstance(meta_obj, dict) else None
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_detail_timeout)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_detail_timeout)")
                total_big += 1
                continue

            db_try(
                logger,
                dsn,
                lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None),
                "audit_upsert(pending)",
            )
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
