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
            if k.lower() == "token" and v:
                qs2.append((k, "****"))
            else:
                qs2.append((k, v))
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs2), p.fragment))
    except Exception:
        return url


def request_with_retry(session: requests.Session, logger: logging.Logger, method: str, url: str, params: dict, timeout: tuple, attempts: int):
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


def list_tickets(session: requests.Session, logger: logging.Logger, token: str, flt: str, select: str, timeout: tuple, attempts: int, top: int | None = None, orderby: str | None = None):
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


def get_ticket_meta(session: requests.Session, logger: logging.Logger, token: str, ticket_id: int, timeout: tuple, attempts: int):
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


def get_ticket_detail(session: requests.Session, logger: logging.Logger, token: str, ticket_id: int, timeout: tuple, attempts: int):
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


def fetch_ticket_for_excluidos(session: requests.Session, logger: logging.Logger, token: str, ticket_id: int, endpoint: str, timeout: tuple, attempts: int):
    params = {
        "token": token,
        "includeDeletedItems": "true",
        "$filter": f"id eq {int(ticket_id)}",
        "$top": "1",
        "$select": "id,isDeleted,lastUpdate,baseStatus",
    }
    try:
        resp = request_with_retry(session, logger, "GET", f"{API_BASE}/{endpoint}", params, timeout, attempts)
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
    if resp.status_code == 404:
        return None, 404, None
    try:
        return None, resp.status_code, resp.text
    except Exception:
        return None, resp.status_code, None


def fetch_by_id_for_excluidos(session: requests.Session, logger: logging.Logger, token: str, ticket_id: int, timeout: tuple, attempts: int):
    t1, sc1, e1 = fetch_ticket_for_excluidos(session, logger, token, ticket_id, "tickets", timeout, attempts)
    if t1 is not None:
        return t1, sc1, e1
    t2, sc2, e2 = fetch_ticket_for_excluidos(session, logger, token, ticket_id, "tickets/past", timeout, attempts)
    if t2 is not None:
        return t2, sc2, e2
    if sc1 in (200, 404) and sc2 in (200, 404):
        return None, 404, None
    return None, sc2 or sc1, e2 or e1


def upsert_ticket_excluido(conn, ticket_id: int, payload: dict):
    last_update = payload.get("lastUpdate") if isinstance(payload, dict) else None
    with conn.cursor() as cur:
        cur.execute(
            "insert into visualizacao_resolvidos.tickets_excluidos (ticket_id, date_excluido, raw) values (%s, coalesce(%s::timestamptz, now()), %s) on conflict (ticket_id) do update set date_excluido=excluded.date_excluido, raw=excluded.raw",
            (int(ticket_id), last_update, Json(payload)),
        )


def is_timeout_like(status_code: int | None, err: str | None):
    if status_code in (408, 429, 500, 502, 503, 504):
        return True
    if not err:
        return False
    e = err.lower()
    return ("readtimeout" in e) or ("timed out" in e) or ("timeout" in e)


def pg_connect(dsn: str):
    return psycopg2.connect(dsn)


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
            if i == 2:
                raise
            time.sleep(1)
            continue


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


def get_table_columns(conn, schema: str, table: str) -> set[str]:
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = %s and table_name = %s
    """
    rows = pg_all(conn, sql, [schema, table])
    return {r[0] for r in rows}


def ensure_detail_table(conn, logger: logging.Logger):
    schema = os.getenv("DETAIL_SCHEMA", "visualizacao_resolvidos")
    table = os.getenv("DETAIL_TABLE", "tickets_resolvidos_detail")
    raw_col = os.getenv("DETAIL_RAW_COL", "raw")

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}
            (
              ticket_id bigint PRIMARY KEY,
              resolved_date timestamptz,
              closed_date timestamptz,
              canceled_date timestamptz,
              ultima_atualizacao timestamptz,
              assunto text,
              fcr boolean,
              reaberturas integer,
              abertura_via text,
              qtd_acoes integer,
              origin text,
              tempo_uteis_segundos bigint,
              tempo_uteis_minutos bigint,
              tempo_uteis_horas bigint,
              tempo_uteis_hhmmss text,
              tempo_corrido_segundos bigint,
              tempo_corrido_minutos bigint,
              tempo_corrido_horas bigint,
              tempo_corrido_hhmmss text,
              dia_semana_br text,
              dia_semana_finalizado_br text,
              service_first_level text,
              service_second_level text,
              service_third_level text,
              base_status text,
              status text,
              empresa_id bigint,
              empresa_nome text,
              codereferenceadditional text,
              cpfcnpj text,
              responsavel text,
              equipe_responsavel text,
              updated_at timestamptz NOT NULL DEFAULT now(),
              is_placeholder boolean NOT NULL DEFAULT false,
              {raw_col} jsonb
            )
            """
        )
    cols = get_table_columns(conn, schema, table)
    if raw_col not in cols:
        raise RuntimeError(f"Coluna raw não encontrada em {schema}.{table}. Defina DETAIL_RAW_COL.")
    logger.info(f"detail: OK detail table {schema}.{table} (raw_col={raw_col})")
    return schema, table, raw_col, True


def ensure_audit_tables(conn, logger: logging.Logger):
    schema = os.getenv("AUDIT_SCHEMA", "visualizacao_resolvidos")

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_run
            (
              id bigserial PRIMARY KEY,
              created_at timestamptz NOT NULL DEFAULT now(),
              window_start timestamptz NOT NULL DEFAULT now(),
              window_end timestamptz NOT NULL DEFAULT now(),
              table_name text NOT NULL DEFAULT 'tickets_resolvidos_detail',
              run_id bigint,
              workflow text,
              job text,
              ref text,
              sha text,
              repo text,
              max_actions integer,
              window_start_id bigint,
              window_end_id bigint,
              window_center_id bigint
            )
            """
        )

        for col_ddl in (
            "ADD COLUMN IF NOT EXISTS total_api integer NOT NULL DEFAULT 0",
            "ADD COLUMN IF NOT EXISTS missing_total integer NOT NULL DEFAULT 0",
        ):
            cur.execute(f"ALTER TABLE {schema}.audit_recent_run {col_ddl}")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_missing
            (
              run_id bigint NOT NULL REFERENCES {schema}.audit_recent_run(id) ON DELETE CASCADE,
              table_name text NOT NULL,
              ticket_id bigint NOT NULL,
              first_seen timestamptz NOT NULL DEFAULT now(),
              last_seen timestamptz NOT NULL DEFAULT now(),
              attempts integer NOT NULL DEFAULT 0,
              last_attempt timestamptz,
              last_status integer,
              last_error text,
              run_started_at timestamptz,
              CONSTRAINT audit_recent_missing_ticket_id_uq UNIQUE (ticket_id)
            )
            """
        )

    logger.info(f"detail: OK audit tables em {schema}")
    return schema


def audit_upsert(conn, audit_fqn: str, ctx: dict, ticket_id: int, status_code: int | None, err_text: str | None):
    cols = get_table_columns(conn, audit_fqn.split(".")[0], audit_fqn.split(".")[1])

    insert_cols = ["ticket_id"]
    placeholders = ["%s"]
    values = [int(ticket_id)]

    def add_param(col: str, val):
        insert_cols.append(col)
        placeholders.append("%s")
        values.append(val)

    if "run_id" in cols:
        add_param("run_id", ctx.get("run_id"))
    if "table_name" in cols:
        add_param("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))
    if "first_seen" in cols:
        add_param("first_seen", ctx.get("run_started_at"))
    if "last_seen" in cols:
        add_param("last_seen", ctx.get("run_started_at"))
    if "attempts" in cols:
        add_param("attempts", 1)
    if "last_attempt" in cols:
        add_param("last_attempt", ctx.get("run_started_at"))
    if "last_status" in cols:
        add_param("last_status", status_code)
    if "last_error" in cols:
        add_param("last_error", err_text)
    if "run_started_at" in cols:
        add_param("run_started_at", ctx.get("run_started_at"))

    set_sql = []
    if "run_id" in cols:
        set_sql.append("run_id = excluded.run_id")
    if "table_name" in cols:
        set_sql.append("table_name = excluded.table_name")
    if "last_seen" in cols:
        set_sql.append("last_seen = excluded.last_seen")
    if "attempts" in cols:
        set_sql.append(f"attempts = {audit_fqn}.attempts + 1")
    if "last_attempt" in cols:
        set_sql.append("last_attempt = excluded.last_attempt")
    if "last_status" in cols:
        set_sql.append("last_status = excluded.last_status")
    if "last_error" in cols:
        set_sql.append("last_error = excluded.last_error")
    if "run_started_at" in cols:
        set_sql.append("run_started_at = excluded.run_started_at")

    sql = f"""
    INSERT INTO {audit_fqn} ({",".join(insert_cols)})
    VALUES ({",".join(placeholders)})
    ON CONFLICT (ticket_id) DO UPDATE
    SET {",".join(set_sql)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)


def audit_delete(conn, audit_fqn: str, ticket_id: int):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {audit_fqn} WHERE ticket_id=%s", [int(ticket_id)])


def audit_create_run(conn, schema: str, ctx: dict):
    audit_run_fqn = f"{schema}.audit_recent_run"
    cols = get_table_columns(conn, schema, "audit_recent_run")

    insert_cols = []
    placeholders = []
    values = []

    def add(col: str, val):
        insert_cols.append(col)
        placeholders.append("%s")
        values.append(val)

    if "window_start" in cols:
        add("window_start", ctx["window_start"])
    if "window_end" in cols:
        add("window_end", ctx["window_end"])
    if "table_name" in cols:
        add("table_name", ctx.get("table_name", "tickets_resolvidos_detail"))
    if "run_id" in cols:
        add("run_id", ctx.get("external_run_id"))
    if "workflow" in cols:
        add("workflow", ctx.get("workflow"))
    if "job" in cols:
        add("job", ctx.get("job"))
    if "ref" in cols:
        add("ref", ctx.get("ref"))
    if "sha" in cols:
        add("sha", ctx.get("sha"))
    if "repo" in cols:
        add("repo", ctx.get("repo"))
    if "max_actions" in cols:
        add("max_actions", ctx.get("max_actions"))
    if "window_start_id" in cols:
        add("window_start_id", ctx.get("window_start_id"))
    if "window_end_id" in cols:
        add("window_end_id", ctx.get("window_end_id"))
    if "window_center_id" in cols:
        add("window_center_id", ctx.get("window_center_id"))

    if insert_cols:
        sql = f"INSERT INTO {audit_run_fqn} ({','.join(insert_cols)}) VALUES ({','.join(placeholders)}) RETURNING id"
        rid = pg_one(conn, sql, values)[0]
        return int(rid)

    sql = f"INSERT INTO {audit_run_fqn} DEFAULT VALUES RETURNING id"
    rid = pg_one(conn, sql)[0]
    return int(rid)


def audit_update_run_stats(conn, schema: str, run_id: int, total_api: int, missing_total: int):
    audit_run_fqn = f"{schema}.audit_recent_run"
    cols = get_table_columns(conn, schema, "audit_recent_run")

    sets = []
    values = []

    def add(col: str, val):
        sets.append(f"{col}=%s")
        values.append(val)

    if "total_api" in cols:
        add("total_api", int(total_api))
    if "missing_total" in cols:
        add("missing_total", int(missing_total))

    if not sets:
        return

    values.append(int(run_id))
    sql = f"UPDATE {audit_run_fqn} SET {','.join(sets)} WHERE id=%s"
    with conn.cursor() as cur:
        cur.execute(sql, values)


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

    raw_is_json = True
    return schema, table, cols, raw_col, raw_is_json


def select_existing_ids(conn, schema: str, table: str, lower: int, upper: int):
    sql = f"SELECT ticket_id FROM {schema}.{table} WHERE ticket_id > %s AND ticket_id <= %s"
    rows = pg_all(conn, sql, [int(lower), int(upper)])
    return {int(r[0]) for r in rows}


def select_pending(conn, audit_fqn: str, limit: int):
    sql = f"SELECT ticket_id, attempts, last_error FROM {audit_fqn} ORDER BY last_seen ASC LIMIT %s"
    rows = pg_all(conn, sql, [int(limit)])
    out = []
    for r in rows:
        tid = int(r[0])
        att = int(r[1] or 0)
        err = str(r[2] or "")
        out.append((tid, att, err))
    return out


def upsert_too_big(conn, ticket_id: int, base_status: str | None, last_update: str | None, cols: set[str], raw_col: str, raw_is_json: bool):
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
    if "ultima_atualizacao" in cols:
        insert_cols.append("ultima_atualizacao")
        placeholders.append("%s")
        values.append(last_update)
    if "base_status" in cols and base_status:
        pass

    set_sql = [f"{raw_col} = excluded.{raw_col}"]
    if "base_status" in cols:
        set_sql.append("base_status = excluded.base_status")
    if "last_update" in cols:
        set_sql.append("last_update = excluded.last_update")
    if "updated_at" in cols:
        set_sql.append("updated_at = excluded.updated_at")
    if "ultima_atualizacao" in cols:
        set_sql.append("ultima_atualizacao = excluded.ultima_atualizacao")

    sql = f"""
    INSERT INTO {schema}.{table} ({",".join(insert_cols)})
    VALUES ({",".join(placeholders)})
    ON CONFLICT (ticket_id) DO UPDATE
    SET {",".join(set_sql)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)


def upsert_detail(conn, ticket: dict, cols: set[str], raw_col: str, raw_is_json: bool):
    ticket_id = int(ticket.get("id"))
    base_status = ticket.get("baseStatus")
    status = ticket.get("status")
    origin = ticket.get("origin")

    subject = ticket.get("subject")
    resolved_in = ticket.get("resolvedIn") or {}
    closed_in = ticket.get("closedIn") or {}
    canceled_in = ticket.get("canceledIn") or {}

    resolved_date = resolved_in.get("date") if isinstance(resolved_in, dict) else None
    closed_date = closed_in.get("date") if isinstance(closed_in, dict) else None
    canceled_date = canceled_in.get("date") if isinstance(canceled_in, dict) else None

    last_update = ticket.get("lastUpdate")
    last_action = ticket.get("lastActionDate")

    service_first = ticket.get("serviceFirstLevel")
    service_second = ticket.get("serviceSecondLevel")
    service_third = ticket.get("serviceThirdLevel")

    actions_count = ticket.get("actionCount")
    reopened_in = ticket.get("reopenedIn") or {}
    reopened_count = reopened_in.get("count") if isinstance(reopened_in, dict) else None

    fcr = None
    if reopened_count is not None:
        try:
            fcr = int(reopened_count) == 0
        except Exception:
            fcr = None

    custom_values = ticket.get("customFieldValues") or []
    empresa_id = None
    empresa_nome = None
    coderef = None
    cpfcnpj = None
    responsavel = None
    equipe = None
    abertura_via = None

    if isinstance(custom_values, list):
        for cv in custom_values:
            if not isinstance(cv, dict):
                continue
            cf = cv.get("customField") if isinstance(cv.get("customField"), dict) else {}
            name = cf.get("name") if isinstance(cf, dict) else cv.get("name")
            val = cv.get("value")
            if not name:
                continue
            n = str(name).strip().lower()
            if n in ("empresa_id", "empresa id"):
                try:
                    empresa_id = int(val) if val is not None and str(val).strip() != "" else None
                except Exception:
                    empresa_id = None
            elif n in ("empresa_nome", "empresa nome"):
                empresa_nome = str(val) if val is not None else None
            elif n in ("codereferenceadditional", "code reference additional"):
                coderef = str(val) if val is not None else None
            elif n in ("cpfcnpj", "cpf/cnpj"):
                cpfcnpj = str(val) if val is not None else None
            elif n in ("responsavel", "responsável"):
                responsavel = str(val) if val is not None else None
            elif n in ("equipe_responsavel", "equipe responsável"):
                equipe = str(val) if val is not None else None
            elif n in ("abertura_via", "abertura via"):
                abertura_via = str(val) if val is not None else None

    raw_val = Json(ticket) if raw_is_json else ticket

    insert_cols = ["ticket_id", raw_col]
    placeholders = ["%s", "%s"]
    values = [ticket_id, raw_val]

    def add(col: str, val):
        if col in cols:
            insert_cols.append(col)
            placeholders.append("%s")
            values.append(val)

    add("resolved_date", resolved_date)
    add("closed_date", closed_date)
    add("canceled_date", canceled_date)
    add("ultima_atualizacao", last_action or last_update)
    add("assunto", subject)
    add("fcr", fcr)
    add("reaberturas", reopened_count)
    add("abertura_via", abertura_via)
    add("qtd_acoes", actions_count)
    add("origin", origin)
    add("service_first_level", service_first)
    add("service_second_level", service_second)
    add("service_third_level", service_third)
    add("base_status", base_status)
    add("status", status)
    add("empresa_id", empresa_id)
    add("empresa_nome", empresa_nome)
    add("codereferenceadditional", coderef)
    add("cpfcnpj", cpfcnpj)
    add("responsavel", responsavel)
    add("equipe_responsavel", equipe)
    if "updated_at" in cols:
        insert_cols.append("updated_at")
        placeholders.append("now()")
    if "is_placeholder" in cols:
        add("is_placeholder", False)

    set_sql = [f"{raw_col} = excluded.{raw_col}"]
    for c in insert_cols:
        if c in ("ticket_id", raw_col):
            continue
        if c == "updated_at":
            set_sql.append("updated_at = excluded.updated_at")
            continue
        set_sql.append(f"{c} = excluded.{c}")
    if "updated_at" in cols and "updated_at" not in insert_cols:
        set_sql.append("updated_at = now()")

    sql = f"""
    INSERT INTO visualizacao_resolvidos.tickets_resolvidos_detail ({",".join(insert_cols)})
    VALUES ({",".join(placeholders)})
    ON CONFLICT (ticket_id) DO UPDATE
    SET {",".join(set_sql)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)


def run_context(max_actions: int):
    window_hours = _env_int("WINDOW_HOURS", 12)
    now_ts = time.time()
    window_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts - 3600 * window_hours))
    window_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts + 3600 * 1))
    external_run_id = _env_int("EXTERNAL_RUN_ID", int(now_ts))
    return {
        "external_run_id": int(external_run_id),
        "table_name": os.getenv("TABLE_NAME") or "tickets_resolvidos",
        "workflow": os.getenv("GITHUB_WORKFLOW"),
        "job": os.getenv("GITHUB_JOB"),
        "ref": os.getenv("GITHUB_REF"),
        "sha": os.getenv("GITHUB_SHA"),
        "repo": os.getenv("GITHUB_REPOSITORY"),
        "max_actions": max_actions,
        "window_start": window_start,
        "window_end": window_end,
    }


def main():
    logging.basicConfig(stream=sys.stdout, level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("sync_resolved_detail")

    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN") or ""
    if not token:
        raise RuntimeError("MOVIDESK_TOKEN is required")

    dsn = os.getenv("NEON_DSN") or ""
    if not dsn:
        raise RuntimeError("NEON_DSN is required")

    timeout_s = _env_int("HTTP_TIMEOUT", 40)
    attempts = _env_int("HTTP_ATTEMPTS", 6)
    timeout = (timeout_s, timeout_s)

    bulk_limit = _env_int("LIMIT", 25)
    missing_limit = _env_int("MISSING_LIMIT", 30)
    max_actions = _env_int("MAX_ACTIONS", 180)
    big_after_attempts = _env_int("BIG_AFTER_ATTEMPTS", 2)

    meta_timeout = (_env_int("META_TIMEOUT", max(10, int(timeout_s / 2))),) * 2
    meta_attempts = _env_int("META_ATTEMPTS", attempts)

    pending_timeout = (_env_int("PENDING_TIMEOUT", timeout_s),) * 2
    pending_attempts = _env_int("PENDING_ATTEMPTS", attempts)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    ctx = run_context(max_actions)
    run_started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ctx["run_started_at"] = run_started_at

    def setup(conn):
        ensure_detail_table(conn, logger)
        audit_schema = ensure_audit_tables(conn, logger)
        rid = audit_create_run(conn, audit_schema, ctx)
        ctx["run_id"] = rid
        return audit_schema

    audit_schema = db_run(dsn, setup)
    audit_fqn = f"{audit_schema}.audit_recent_missing"

    detail_schema, detail_table, detail_cols, raw_col, raw_is_json = db_run(dsn, lambda conn: get_detail_table_info(conn))
    detail_fqn = f"{detail_schema}.{detail_table}"

    last_id = db_run(dsn, lambda conn: (pg_one(conn, f"select coalesce(max(ticket_id), 0) from {detail_fqn}") or [0])[0]) or 0

    lower = max(0, int(last_id) - 2500)
    upper = int(last_id) + 2500
    ctx["window_start_id"] = lower + 1
    ctx["window_end_id"] = upper
    ctx["window_center_id"] = int(last_id)

    db_ids = db_run(dsn, lambda conn: select_existing_ids(conn, detail_schema, detail_table, lower, upper))

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

    total_ok = 0
    total_missing = 0
    total_big = 0
    total_window = 0
    total_new = 0

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
            chk, csc, ce = fetch_by_id_for_excluidos(session, logger, token, tid, timeout, attempts)
            if (chk is None and csc == 404) or (isinstance(chk, dict) and chk.get("isDeleted") is True):
                payload = chk if isinstance(chk, dict) else {"id": int(tid), "api_not_found": True, "checkedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                db_try(logger, dsn, lambda conn: upsert_ticket_excluido(conn, tid, payload), "upsert_ticket_excluido(window)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window_excluido)")
                continue
            db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(window)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(window)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(window)")
        total_ok += 1
        total_window += 1

    flt_new = (
        f"id gt {int(last_id)} and "
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
            chk, csc, ce = fetch_by_id_for_excluidos(session, logger, token, tid, timeout, attempts)
            if (chk is None and csc == 404) or (isinstance(chk, dict) and chk.get("isDeleted") is True):
                payload = chk if isinstance(chk, dict) else {"id": int(tid), "api_not_found": True, "checkedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                db_try(logger, dsn, lambda conn: upsert_ticket_excluido(conn, tid, payload), "upsert_ticket_excluido(new)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new_excluido)")
                continue
            db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(new)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(new)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(new)")
        total_ok += 1
        total_new += 1

    pend_ids = db_run(dsn, lambda conn: select_pending(conn, audit_fqn, missing_limit))
    logger.info(f"detail: {len(pend_ids)} tickets pendentes na fila audit_recent_missing (limite={missing_limit}).")

    for idx, (tid, prev_attempts, prev_err) in enumerate(pend_ids, start=1):
        logger.info(f"detail: Processando ticket pendente {idx}/{len(pend_ids)} (ID={tid})")

        if prev_attempts >= big_after_attempts and is_timeout_like(None, prev_err):
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_by_attempts)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_by_attempts)")
            total_big += 1
            continue

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

            chk, csc, ce = fetch_by_id_for_excluidos(session, logger, token, tid, pending_timeout, pending_attempts)
            if (chk is None and csc == 404) or (isinstance(chk, dict) and chk.get("isDeleted") is True):
                payload = chk if isinstance(chk, dict) else {"id": int(tid), "api_not_found": True, "checkedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                db_try(logger, dsn, lambda conn: upsert_ticket_excluido(conn, tid, payload), "upsert_ticket_excluido(pending)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending_excluido)")
                continue

            db_try(logger, dsn, lambda conn: audit_upsert(conn, audit_fqn, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(pending)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(pending)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, audit_fqn, tid), "audit_delete(pending)")
        total_ok += 1

    db_try(logger, dsn, lambda conn: audit_update_run_stats(conn, audit_schema, int(ctx["run_id"]), len(api_ids) + len(new_ids), total_missing), "audit_update_run_stats")

    logger.info(
        f"detail: Finalizado. OK={total_ok} (janela={total_window}, novos={total_new}), "
        f"Muito_grandes={total_big}, Falhas={total_missing}."
    )


if __name__ == "__main__":
    main()
