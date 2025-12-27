import os
import sys
import time
import logging
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
import psycopg2
from psycopg2.extras import Json


API_BASE = "https://api.movidesk.com/public/v1"

SELECT_LIST = "id,baseStatus,lastUpdate,isDeleted"
SELECT_DETAIL = (
    "id,protocol,type,subject,category,urgency,status,baseStatus,justification,origin,"
    "createdDate,isDeleted,owner,ownerTeam,createdBy,serviceFull,serviceFirstLevel,"
    "serviceSecondLevel,serviceThirdLevel,contactForm,tags,cc,resolvedIn,closedIn,"
    "canceledIn,actionCount,reopenedIn,lastActionDate,lastUpdate,clients,statusHistories,"
    "customFieldValues"
)

BASE_STATUSES = ("Resolved", "Closed", "Canceled")


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


def request_with_retry(session: requests.Session, logger: logging.Logger, method: str, url: str, params: dict, timeout: tuple, attempts: int):
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            req = requests.Request(method=method, url=url, params=params)
            prepped = session.prepare_request(req)
            masked = mask_token(prepped.url)
            resp = session.send(prepped, timeout=timeout)
            logger.info(f"GET {masked} -> {resp.status_code} (attempt {i}/{attempts})")
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(30, 2 ** (i - 1)))
                continue
            return resp
        except Exception as e:
            last_exc = e
            logger.info(f"GET {mask_token(url)} -> EXC (attempt {i}/{attempts}): {type(e).__name__}: {e}")
            time.sleep(min(30, 2 ** (i - 1)))
            continue
    raise last_exc if last_exc else RuntimeError("request failed")


def pg_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchone()


def pg_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()


def ensure_audit_table(conn, logger: logging.Logger):
    schema = os.getenv("AUDIT_SCHEMA", "visualizacao_resolvidos")
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.audit_recent_missing
            (
              ticket_id bigint PRIMARY KEY,
              run_id bigint NOT NULL DEFAULT 0,
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

        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS run_id bigint")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS run_started_at timestamptz")

        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ALTER COLUMN run_id SET DEFAULT 0")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ALTER COLUMN run_started_at SET DEFAULT now()")

        cur.execute(f"UPDATE {schema}.audit_recent_missing SET run_id = COALESCE(run_id, 0)")
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET run_started_at = COALESCE(run_started_at, now())")

        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ALTER COLUMN run_id SET NOT NULL")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ALTER COLUMN run_started_at SET NOT NULL")

        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS first_seen timestamptz NOT NULL DEFAULT now()")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_seen timestamptz NOT NULL DEFAULT now()")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_attempt timestamptz")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_status integer")
        cur.execute(f"ALTER TABLE {schema}.audit_recent_missing ADD COLUMN IF NOT EXISTS last_error text")

        cur.execute(f"UPDATE {schema}.audit_recent_missing SET attempts=0 WHERE attempts IS NULL")
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET first_seen=now() WHERE first_seen IS NULL")
        cur.execute(f"UPDATE {schema}.audit_recent_missing SET last_seen=now() WHERE last_seen IS NULL")

        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS audit_recent_missing_ticket_id_uq ON {schema}.audit_recent_missing(ticket_id)")
    logger.info(f"detail: audit_recent_missing em {schema}.audit_recent_missing")
    return f"{schema}.audit_recent_missing"


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


def upsert_detail(conn, ticket: dict):
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

    insert_cols = ["ticket_id", raw_col]
    placeholders = ["%s", "%s"]
    values = [int(ticket.get("id")), Json(ticket)]

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


def audit_upsert(conn, audit_fqn: str, ticket_id: int, status_code: int | None, error_text: str | None):
    schema, table = audit_fqn.split(".", 1)
    run_id = _env_int("GITHUB_RUN_ID", _env_int("RUN_ID", 0))

    sql = f"""
    INSERT INTO {schema}.{table}
      (ticket_id, run_id, run_started_at, first_seen, last_seen, attempts, last_attempt, last_status, last_error)
    VALUES
      (%s, %s, now(), now(), now(), 1, now(), %s, %s)
    ON CONFLICT (ticket_id) DO UPDATE SET
      run_id = EXCLUDED.run_id,
      run_started_at = COALESCE({schema}.{table}.run_started_at, EXCLUDED.run_started_at),
      last_seen = now(),
      attempts = COALESCE({schema}.{table}.attempts,0) + 1,
      last_attempt = now(),
      last_status = EXCLUDED.last_status,
      last_error = EXCLUDED.last_error
    """
    with conn.cursor() as cur:
        cur.execute(sql, [int(ticket_id), run_id, status_code, error_text])


def audit_delete(conn, audit_fqn: str, ticket_id: int):
    schema, table = audit_fqn.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{table} WHERE ticket_id=%s", [int(ticket_id)])


def list_tickets(session: requests.Session, logger: logging.Logger, token: str, flt: str, select: str, timeout: tuple, attempts: int, top: int | None = None, orderby: str | None = None):
    params = {"$filter": flt, "$select": select, "includeDeletedItems": "true", "token": token}
    if top is not None:
        params["$top"] = str(top)
    if orderby:
        params["$orderby"] = orderby
    resp = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets", params, timeout, attempts)
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


def get_ticket_detail(session: requests.Session, logger: logging.Logger, token: str, ticket_id: int, timeout: tuple, attempts: int):
    params = {"id": str(ticket_id), "$select": SELECT_DETAIL, "includeDeletedItems": "true", "token": token}
    resp = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets", params, timeout, attempts)
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
    resp2 = request_with_retry(session, logger, "GET", f"{API_BASE}/tickets/past", params2, timeout, attempts)
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


def main():
    logger = setup_logger()

    dsn = os.getenv("NEON_DSN")
    token = os.getenv("MOVIDESK_TOKEN")
    if not dsn or not token:
        raise SystemExit("NEON_DSN e MOVIDESK_TOKEN são obrigatórios")

    bulk_limit = int(os.getenv("DETAIL_BULK_LIMIT", "200"))
    missing_limit = int(os.getenv("DETAIL_MISSING_LIMIT", "10"))
    window = int(os.getenv("DETAIL_WINDOW", "50"))
    attempts = int(os.getenv("DETAIL_ATTEMPTS", "6"))
    connect_timeout = int(os.getenv("DETAIL_CONNECT_TIMEOUT", "10"))
    read_timeout = int(os.getenv("DETAIL_READ_TIMEOUT", "120"))
    timeout = (connect_timeout, read_timeout)

    logger.info(f"detail: Iniciando sync (bulk={bulk_limit}, missing={missing_limit}, window={window}, timeout=({connect_timeout}s,{read_timeout}s), attempts={attempts}).")

    def load_state(conn):
        audit_fqn = ensure_audit_table(conn, logger)

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

        pend = pg_all(conn, f"SELECT ticket_id FROM {audit_fqn} ORDER BY ticket_id DESC LIMIT %s", [missing_limit])
        pend_ids = [int(r[0]) for r in pend]

        return audit_fqn, last_id, lower, upper, db_ids, pend_ids

    audit_fqn, last_id, lower, upper, db_ids, pend_ids = db_run(dsn, load_state)
    logger.info(f"detail: Último ticket_id em tickets_resolvidos_detail: {last_id}")

    session = requests.Session()

    total_ok = 0
    total_new = 0
    total_window = 0
    total_missing = 0

    flt_window = (
        f"id gt {lower} and id le {upper} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )

    logger.info(f"detail: Janela: /tickets com id > {lower} e <= {upper} (Resolved/Closed/Canceled)")
    window_items, _, _ = list_tickets(session, logger, token, flt_window, SELECT_LIST, timeout, attempts, top=1000, orderby="id")
    api_ids = {int(x.get("id")) for x in window_items if isinstance(x, dict) and x.get("id") is not None}

    to_fetch = sorted(list(api_ids - db_ids))
    logger.info(f"detail: Janela (center={last_id}): encontrados={len(api_ids)}, já_existiam={len(db_ids)}, para_buscar={len(to_fetch)}")

    for idx, tid in enumerate(to_fetch[: max(0, bulk_limit)], start=1):
        logger.info(f"detail: Janela: buscando detalhe {idx}/{min(len(to_fetch), bulk_limit)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            db_run(dsn, lambda conn: audit_upsert(conn, audit_fqn, tid, sc, (e or "")[:4000] if e else None))
            total_missing += 1
            continue
        db_run(dsn, lambda conn: upsert_detail(conn, ticket))
        db_run(dsn, lambda conn: audit_delete(conn, audit_fqn, tid))
        total_ok += 1
        total_window += 1

    flt_new = (
        f"id gt {last_id} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )
    new_items, _, _ = list_tickets(session, logger, token, flt_new, SELECT_LIST, timeout, attempts, top=bulk_limit, orderby="id")
    new_ids = [int(x.get("id")) for x in new_items if isinstance(x, dict) and x.get("id") is not None]

    logger.info(f"detail: {len(new_ids)} tickets em /tickets com id > {last_id} (Resolved/Closed/Canceled)")
    for idx, tid in enumerate(new_ids, start=1):
        logger.info(f"detail: Novo: buscando detalhe {idx}/{len(new_ids)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            db_run(dsn, lambda conn: audit_upsert(conn, audit_fqn, tid, sc, (e or "")[:4000] if e else None))
            total_missing += 1
            continue
        db_run(dsn, lambda conn: upsert_detail(conn, ticket))
        db_run(dsn, lambda conn: audit_delete(conn, audit_fqn, tid))
        total_ok += 1
        total_new += 1

    logger.info(f"detail: {len(pend_ids)} tickets pendentes na fila audit_recent_missing (limite={missing_limit}).")
    for idx, tid in enumerate(pend_ids, start=1):
        logger.info(f"detail: Processando ticket pendente {idx}/{len(pend_ids)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            db_run(dsn, lambda conn: audit_upsert(conn, audit_fqn, tid, sc, (e or "")[:4000] if e else None))
            total_missing += 1
            continue
        db_run(dsn, lambda conn: upsert_detail(conn, ticket))
        db_run(dsn, lambda conn: audit_delete(conn, audit_fqn, tid))
        total_ok += 1

    logger.info(f"detail: Finalizado. OK={total_ok} (janela={total_window}, novos={total_new}), Falhas={total_missing}.")


if __name__ == "__main__":
    main()
