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
    "id,protocol,type,subject,category,urgency,status,baseStatus,justification,createdDate,createdBy,owner,ownerTeam,actions,customFieldValues"
)

DEFAULT_TIMEOUT = 40
DEFAULT_ATTEMPTS = 3
DEFAULT_BULK_LIMIT = 25
DEFAULT_MISSING_LIMIT = 30
DEFAULT_MAX_ACTIONS = 180
DEFAULT_BIG_AFTER_ATTEMPTS = 2

AUDIT_SCHEMA = "visualizacao_resolvidos"
AUDIT_RUN_TABLE = "audit_recent_run"
AUDIT_MISSING_TABLE = "audit_recent_missing"
DETAIL_SCHEMA = "visualizacao_resolvidos"
DETAIL_TABLE = "tickets_resolvidos_detail"
TOO_BIG_TABLE = "tickets_excluidos"

DETAIL_FQN = f"{DETAIL_SCHEMA}.{DETAIL_TABLE}"
AUDIT_RUN_FQN = f"{AUDIT_SCHEMA}.{AUDIT_RUN_TABLE}"
AUDIT_MISSING_FQN = f"{AUDIT_SCHEMA}.{AUDIT_MISSING_TABLE}"
TOO_BIG_FQN = f"{AUDIT_SCHEMA}.{TOO_BIG_TABLE}"


def env_int(name, default):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def env_float(name, default):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def normalize_url(base, token):
    p = urlparse(base)
    qs = dict(parse_qsl(p.query))
    if "token" not in qs and token:
        qs["token"] = token
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs), p.fragment))


def session_for():
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


def is_timeout_like(status_code, err):
    if err:
        se = str(err).lower()
        if "timed out" in se or "timeout" in se or "read timed out" in se:
            return True
        if "connection aborted" in se or "connection reset" in se:
            return True
        if "temporary failure" in se or "name or service not known" in se:
            return True
    if status_code in (408, 429, 500, 502, 503, 504):
        return True
    return False


def http_get(session, url, timeout, attempts, logger):
    last_err = None
    for i in range(max(1, attempts)):
        try:
            r = session.get(url, timeout=timeout)
            sc = r.status_code
            if 200 <= sc < 300:
                try:
                    return r.json(), sc, None
                except Exception as e:
                    return None, sc, e
            try:
                body = r.text
            except Exception:
                body = ""
            last_err = body or f"HTTP {sc}"
            if sc == 404:
                return None, sc, last_err
            if is_timeout_like(sc, last_err) and i < attempts - 1:
                time.sleep(1 + i)
                continue
            return None, sc, last_err
        except Exception as e:
            last_err = e
            if is_timeout_like(None, e) and i < attempts - 1:
                time.sleep(1 + i)
                continue
            return None, None, e
    return None, None, last_err


def list_tickets(session, logger, token, flt, select, timeout, attempts, top=1000, orderby="id"):
    url = normalize_url(f"{API_BASE}/tickets", token)
    params = {
        "$select": select,
        "$filter": flt,
        "$top": str(top),
        "$orderby": orderby,
    }
    u = url + ("&" if "?" in url else "?") + urlencode(params)
    data, sc, e = http_get(session, u, timeout, attempts, logger)
    if isinstance(data, list):
        return data, sc, e
    return [], sc, e


def get_ticket_meta(session, logger, token, tid, timeout, attempts):
    url = normalize_url(f"{API_BASE}/tickets", token)
    params = {"$select": SELECT_META, "$filter": f"id eq {int(tid)}", "$top": "1"}
    u = url + ("&" if "?" in url else "?") + urlencode(params)
    data, sc, e = http_get(session, u, timeout, attempts, logger)
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0], sc, e
    if sc == 404:
        return None, 404, e
    return None, sc, e


def get_ticket_detail(session, logger, token, tid, timeout, attempts):
    url = normalize_url(f"{API_BASE}/tickets/{int(tid)}", token)
    params = {"$select": SELECT_DETAIL}
    u = url + ("&" if "?" in url else "?") + urlencode(params)
    data, sc, e = http_get(session, u, timeout, attempts, logger)
    if isinstance(data, dict) and data.get("id") is not None:
        return data, sc, e
    if sc == 404:
        return None, 404, e
    return None, sc, e


def connect(dsn):
    return psycopg2.connect(dsn)


def db_try(logger, dsn, fn, label, retries=5, sleep_s=0.6):
    last = None
    for i in range(max(1, retries)):
        try:
            with connect(dsn) as conn:
                with conn.cursor() as cur:
                    return fn(conn)
        except Exception as e:
            last = e
            se = str(e).lower()
            if ("deadlock detected" in se or "could not serialize access" in se or "canceling statement due to lock timeout" in se) and i < retries - 1:
                time.sleep(sleep_s * (1 + i))
                continue
            logger.error(f"{label}: {e}")
            raise
    raise last


def ensure_tables(conn, detail_fqn, audit_run_fqn, audit_missing_fqn, too_big_fqn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            create table if not exists {audit_run_fqn}(
              id bigserial primary key,
              window_start timestamptz,
              window_end timestamptz,
              created_at timestamptz not null default now(),
              table_name text not null default 'tickets_resolvidos',
              total_api integer not null default 0,
              missing_total integer not null default 0
            );
            """
        )
        cur.execute(
            f"""
            create table if not exists {audit_missing_fqn}(
              run_id bigint not null references {audit_run_fqn}(id) on delete cascade,
              table_name text not null,
              ticket_id bigint not null,
              first_seen timestamptz not null default now(),
              last_seen timestamptz not null default now(),
              attempts integer not null default 0,
              last_attempt timestamptz,
              last_status integer,
              last_error text,
              run_started_at timestamptz,
              constraint audit_recent_missing_ticket_id_uq unique (ticket_id)
            );
            """
        )
        cur.execute(
            f"""
            create table if not exists {detail_fqn}(
              ticket_id bigint primary key,
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
              updated_at timestamptz not null default now(),
              is_placeholder boolean not null default false,
              raw jsonb
            );
            """
        )
        cur.execute(
            f"""
            create table if not exists {too_big_fqn}(
              ticket_id bigint primary key,
              base_status text,
              last_update timestamptz,
              created_at timestamptz not null default now(),
              reason text,
              raw jsonb
            );
            """
        )


def audit_upsert(conn, audit_fqn, ctx, ticket_id, status_code, err_text):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {audit_fqn}
              (run_id, table_name, ticket_id, first_seen, last_seen, attempts, last_attempt, last_status, last_error, run_started_at)
            values
              (%s, %s, %s, now(), now(), 1, now(), %s, %s, %s)
            on conflict (ticket_id)
            do update set
              run_id = excluded.run_id,
              table_name = excluded.table_name,
              last_seen = excluded.last_seen,
              attempts = {audit_fqn}.attempts + 1,
              last_attempt = excluded.last_attempt,
              last_status = excluded.last_status,
              last_error = excluded.last_error,
              run_started_at = excluded.run_started_at;
            """,
            (
                ctx.get("run_id"),
                ctx.get("table_name", "tickets_resolvidos"),
                int(ticket_id),
                status_code if status_code is not None else None,
                err_text,
                ctx.get("run_started_at"),
            ),
        )


def audit_delete(conn, audit_fqn, ticket_id):
    with conn.cursor() as cur:
        cur.execute(f"delete from {audit_fqn} where ticket_id = %s;", (int(ticket_id),))


def create_run(conn, audit_run_fqn, window_start, window_end, table_name):
    with conn.cursor() as cur:
        cur.execute(
            f"insert into {audit_run_fqn}(window_start, window_end, table_name) values (%s, %s, %s) returning id;",
            (window_start, window_end, table_name),
        )
        rid = cur.fetchone()[0]
        return int(rid)


def update_run_stats(conn, audit_run_fqn, run_id, total_api, missing_total):
    with conn.cursor() as cur:
        cur.execute(
            f"update {audit_run_fqn} set total_api=%s, missing_total=%s where id=%s;",
            (int(total_api), int(missing_total), int(run_id)),
        )


def select_existing_ids(conn, detail_fqn, lower, upper):
    with conn.cursor() as cur:
        cur.execute(
            f"select ticket_id from {detail_fqn} where ticket_id > %s and ticket_id <= %s;",
            (int(lower), int(upper)),
        )
        return {int(r[0]) for r in cur.fetchall()}


def select_pending(conn, audit_fqn, missing_limit):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select ticket_id, attempts, last_error
            from {audit_fqn}
            order by last_seen asc
            limit %s;
            """,
            (int(missing_limit),),
        )
        return cur.fetchall()


def upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json):
    tid = int(ticket.get("id"))
    subject = ticket.get("subject")
    status = ticket.get("status")
    base_status = ticket.get("baseStatus")
    created_date = ticket.get("createdDate")
    updated_date = ticket.get("lastUpdate") or ticket.get("updatedDate")
    justification = ticket.get("justification")
    protocol = ticket.get("protocol")
    ttype = ticket.get("type")
    category = ticket.get("category")
    urgency = ticket.get("urgency")
    owner = ticket.get("owner")
    owner_team = ticket.get("ownerTeam")
    created_by = ticket.get("createdBy")

    custom = ticket.get("customFieldValues") or []
    empresa_id = None
    empresa_nome = None
    code_ref = None
    cpfcnpj = None
    responsavel = None
    equipe = None
    abertura_via = None
    origin = None

    if isinstance(custom, list):
        for c in custom:
            if not isinstance(c, dict):
                continue
            name = (c.get("customField") or {}).get("name") if isinstance(c.get("customField"), dict) else c.get("name")
            value = c.get("value")
            if not name:
                continue
            n = str(name).strip().lower()
            if n in ("empresa_id", "empresa id"):
                try:
                    empresa_id = int(value) if value is not None and str(value).strip() != "" else None
                except Exception:
                    empresa_id = None
            elif n in ("empresa_nome", "empresa nome"):
                empresa_nome = str(value) if value is not None else None
            elif n in ("codereferenceadditional", "code reference additional"):
                code_ref = str(value) if value is not None else None
            elif n in ("cpfcnpj", "cpf/cnpj"):
                cpfcnpj = str(value) if value is not None else None
            elif n in ("responsavel", "responsável"):
                responsavel = str(value) if value is not None else None
            elif n in ("equipe_responsavel", "equipe responsável"):
                equipe = str(value) if value is not None else None
            elif n in ("abertura_via", "abertura via"):
                abertura_via = str(value) if value is not None else None
            elif n in ("origin",):
                origin = str(value) if value is not None else None

    actions = ticket.get("actions") or []
    action_count = len(actions) if isinstance(actions, list) else None
    reaberturas = None
    fcr = None
    resolved_date = None
    closed_date = None
    canceled_date = None
    ultima_atualizacao = None

    if isinstance(actions, list):
        reab = 0
        for a in actions:
            if not isinstance(a, dict):
                continue
            at = a.get("type")
            if str(at).lower() == "reopen":
                reab += 1
            if resolved_date is None and str(at).lower() == "resolve":
                resolved_date = a.get("date")
            if closed_date is None and str(at).lower() == "close":
                closed_date = a.get("date")
            if canceled_date is None and str(at).lower() == "cancel":
                canceled_date = a.get("date")
            ultima_atualizacao = a.get("date") or ultima_atualizacao
        reaberturas = reab
        if reaberturas == 0:
            fcr = True
        else:
            fcr = False

    tempo_uteis_segundos = None
    tempo_uteis_minutos = None
    tempo_uteis_horas = None
    tempo_uteis_hhmmss = None
    tempo_corrido_segundos = None
    tempo_corrido_minutos = None
    tempo_corrido_horas = None
    tempo_corrido_hhmmss = None
    dia_semana_br = None
    dia_semana_finalizado_br = None
    service_first_level = None
    service_second_level = None
    service_third_level = None

    raw_value = Json(ticket) if raw_is_json else ticket

    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {DETAIL_FQN}(
              ticket_id,resolved_date,closed_date,canceled_date,ultima_atualizacao,assunto,fcr,reaberturas,
              abertura_via,qtd_acoes,origin,
              tempo_uteis_segundos,tempo_uteis_minutos,tempo_uteis_horas,tempo_uteis_hhmmss,
              tempo_corrido_segundos,tempo_corrido_minutos,tempo_corrido_horas,tempo_corrido_hhmmss,
              dia_semana_br,dia_semana_finalizado_br,
              service_first_level,service_second_level,service_third_level,
              base_status,status,
              empresa_id,empresa_nome,codereferenceadditional,cpfcnpj,responsavel,equipe_responsavel,
              updated_at,is_placeholder,raw
            )
            values(
              %s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,
              %s,%s,%s,
              %s,%s,
              %s,%s,%s,%s,%s,%s,
              now(),false,%s
            )
            on conflict (ticket_id)
            do update set
              resolved_date=excluded.resolved_date,
              closed_date=excluded.closed_date,
              canceled_date=excluded.canceled_date,
              ultima_atualizacao=excluded.ultima_atualizacao,
              assunto=excluded.assunto,
              fcr=excluded.fcr,
              reaberturas=excluded.reaberturas,
              abertura_via=excluded.abertura_via,
              qtd_acoes=excluded.qtd_acoes,
              origin=excluded.origin,
              tempo_uteis_segundos=excluded.tempo_uteis_segundos,
              tempo_uteis_minutos=excluded.tempo_uteis_minutos,
              tempo_uteis_horas=excluded.tempo_uteis_horas,
              tempo_uteis_hhmmss=excluded.tempo_uteis_hhmmss,
              tempo_corrido_segundos=excluded.tempo_corrido_segundos,
              tempo_corrido_minutos=excluded.tempo_corrido_minutos,
              tempo_corrido_horas=excluded.tempo_corrido_horas,
              tempo_corrido_hhmmss=excluded.tempo_corrido_hhmmss,
              dia_semana_br=excluded.dia_semana_br,
              dia_semana_finalizado_br=excluded.dia_semana_finalizado_br,
              service_first_level=excluded.service_first_level,
              service_second_level=excluded.service_second_level,
              service_third_level=excluded.service_third_level,
              base_status=excluded.base_status,
              status=excluded.status,
              empresa_id=excluded.empresa_id,
              empresa_nome=excluded.empresa_nome,
              codereferenceadditional=excluded.codereferenceadditional,
              cpfcnpj=excluded.cpfcnpj,
              responsavel=excluded.responsavel,
              equipe_responsavel=excluded.equipe_responsavel,
              updated_at=now(),
              is_placeholder=false,
              raw=excluded.raw;
            """,
            (
                tid,
                resolved_date,
                closed_date,
                canceled_date,
                ultima_atualizacao,
                subject,
                fcr,
                reaberturas,
                abertura_via,
                action_count,
                origin,
                tempo_uteis_segundos,
                tempo_uteis_minutos,
                tempo_uteis_horas,
                tempo_uteis_hhmmss,
                tempo_corrido_segundos,
                tempo_corrido_minutos,
                tempo_corrido_horas,
                tempo_corrido_hhmmss,
                dia_semana_br,
                dia_semana_finalizado_br,
                service_first_level,
                service_second_level,
                service_third_level,
                base_status,
                status,
                empresa_id,
                empresa_nome,
                code_ref,
                cpfcnpj,
                responsavel,
                equipe,
                raw_value,
            ),
        )


def upsert_too_big(conn, ticket_id, base_status, last_update, detail_cols, raw_col, raw_is_json):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {TOO_BIG_FQN}(ticket_id, base_status, last_update, reason, raw)
            values (%s, %s, %s, %s, %s)
            on conflict (ticket_id)
            do update set base_status=excluded.base_status, last_update=excluded.last_update, reason=excluded.reason, raw=excluded.raw;
            """,
            (
                int(ticket_id),
                base_status,
                last_update,
                "too_many_actions_or_timeout",
                Json({"ticket_id": int(ticket_id), "base_status": base_status, "last_update": last_update}) if raw_is_json else {"ticket_id": int(ticket_id)},
            ),
        )


def run_context(external_run_id, table_name):
    return {
        "external_run_id": int(external_run_id),
        "table_name": table_name,
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

    timeout = env_int("HTTP_TIMEOUT", DEFAULT_TIMEOUT)
    attempts = env_int("HTTP_ATTEMPTS", DEFAULT_ATTEMPTS)

    meta_timeout = env_int("META_TIMEOUT", max(10, int(timeout / 2)))
    meta_attempts = env_int("META_ATTEMPTS", attempts)

    pending_timeout = env_int("PENDING_TIMEOUT", timeout)
    pending_attempts = env_int("PENDING_ATTEMPTS", attempts)

    bulk_limit = env_int("LIMIT", DEFAULT_BULK_LIMIT)
    missing_limit = env_int("MISSING_LIMIT", DEFAULT_MISSING_LIMIT)
    max_actions = env_int("MAX_ACTIONS", DEFAULT_MAX_ACTIONS)
    big_after_attempts = env_int("BIG_AFTER_ATTEMPTS", DEFAULT_BIG_AFTER_ATTEMPTS)

    external_run_id = env_int("EXTERNAL_RUN_ID", int(time.time()))
    table_name = os.getenv("TABLE_NAME") or "tickets_resolvidos"

    ctx = run_context(external_run_id, table_name)

    lock_timeout_ms = env_int("LOCK_TIMEOUT_MS", 5000)
    statement_timeout_ms = env_int("STATEMENT_TIMEOUT_MS", 300000)
    txn_retries = env_int("TXN_RETRIES", 6)

    raw_col = "raw"
    raw_is_json = True

    session = session_for()

    def setup(conn):
        with conn.cursor() as cur:
            cur.execute(f"set lock_timeout = {int(lock_timeout_ms)};")
            cur.execute(f"set statement_timeout = {int(statement_timeout_ms)};")
        ensure_tables(conn, DETAIL_FQN, AUDIT_RUN_FQN, AUDIT_MISSING_FQN, TOO_BIG_FQN)

    db_try(logger, dsn, setup, "setup", retries=txn_retries)

    window_start = None
    window_end = None

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select coalesce(max(ticket_id), 0) from {DETAIL_FQN};")
            last_id = int(cur.fetchone()[0] or 0)

    now_ts = int(time.time())
    window_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts - 3600 * 12))
    window_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts + 3600 * 1))

    def create_run_tx(conn):
        rid = create_run(conn, AUDIT_RUN_FQN, window_start, window_end, table_name)
        return rid

    run_id = db_try(logger, dsn, create_run_tx, "create_run", retries=txn_retries)
    ctx["run_id"] = run_id
    ctx["run_started_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    lower = max(0, last_id - 2500)
    upper = last_id + 2500

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select coalesce(max(ticket_id), 0) from {DETAIL_FQN};")
            last_id = int(cur.fetchone()[0] or 0)

    def load_db_ids(conn):
        return select_existing_ids(conn, DETAIL_FQN, lower, upper)

    db_ids = db_try(logger, dsn, load_db_ids, "select_existing_ids", retries=txn_retries)

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

    detail_cols = None

    for idx, tid in enumerate(to_fetch[: max(0, bulk_limit)], start=1):
        m = meta_map.get(tid) or {}
        ac = m.get("actionCount")
        bs = m.get("baseStatus")
        lu = m.get("lastUpdate")

        if isinstance(ac, int) and ac > max_actions:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(window)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(window)")
            total_big += 1
            total_window += 1
            continue

        logger.info(f"detail: Janela: buscando detalhe {idx}/{min(len(to_fetch), bulk_limit)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            if sc == 404:
                db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(window_not_found)")
                continue
            db_try(logger, dsn, lambda conn: audit_upsert(conn, AUDIT_MISSING_FQN, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(window)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(window)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(window)")
        total_ok += 1
        total_window += 1

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
            db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(new)")
            total_big += 1
            total_new += 1
            continue

        logger.info(f"detail: Novo: buscando detalhe {idx}/{len(new_ids)} (ID={tid})")
        ticket, sc, e = get_ticket_detail(session, logger, token, tid, timeout, attempts)
        if ticket is None:
            if sc == 404:
                db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(new_not_found)")
                continue
            db_try(logger, dsn, lambda conn: audit_upsert(conn, AUDIT_MISSING_FQN, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(new)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(new)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(new)")
        total_ok += 1
        total_new += 1

    def fetch_pending(conn):
        return select_pending(conn, AUDIT_MISSING_FQN, missing_limit)

    pend_rows = db_try(logger, dsn, fetch_pending, "select_pending", retries=txn_retries)
    pend_ids = [(int(r[0]), int(r[1] or 0), str(r[2] or "")) for r in pend_rows]
    logger.info(f"detail: {len(pend_ids)} tickets pendentes na fila audit_recent_missing (limite={missing_limit}).")

    for idx, (tid, prev_attempts, prev_err) in enumerate(pend_ids, start=1):
        logger.info(f"detail: Processando ticket pendente {idx}/{len(pend_ids)} (ID={tid})")

        if prev_attempts >= big_after_attempts and is_timeout_like(None, prev_err):
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_by_attempts)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending_by_attempts)")
            total_big += 1
            continue

        meta_obj, msc, me = get_ticket_meta(session, logger, token, tid, meta_timeout, meta_attempts)
        if isinstance(meta_obj, dict):
            ac = meta_obj.get("actionCount")
            bs = meta_obj.get("baseStatus")
            lu = meta_obj.get("lastUpdate")
            if isinstance(ac, int) and ac > max_actions:
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending_meta)")
                total_big += 1
                continue

        if meta_obj is None and is_timeout_like(msc, me) and (prev_attempts + 1) >= big_after_attempts:
            db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, None, None, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_meta_timeout)")
            db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending_meta_timeout)")
            total_big += 1
            continue

        ticket, sc, e = get_ticket_detail(session, logger, token, tid, pending_timeout, pending_attempts)
        if ticket is None:
            if sc == 404:
                db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending_not_found)")
                continue
            if is_timeout_like(sc, e) and (prev_attempts + 1) >= big_after_attempts:
                bs = meta_obj.get("baseStatus") if isinstance(meta_obj, dict) else None
                lu = meta_obj.get("lastUpdate") if isinstance(meta_obj, dict) else None
                db_try(logger, dsn, lambda conn: upsert_too_big(conn, tid, bs, lu, detail_cols, raw_col, raw_is_json), "upsert_too_big(pending_detail_timeout)")
                db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending_detail_timeout)")
                total_big += 1
                continue

            db_try(logger, dsn, lambda conn: audit_upsert(conn, AUDIT_MISSING_FQN, ctx, tid, sc, (e or "")[:4000] if e else None), "audit_upsert(pending)")
            total_missing += 1
            continue

        db_try(logger, dsn, lambda conn: upsert_detail(conn, ticket, detail_cols, raw_col, raw_is_json), "upsert_detail(pending)")
        db_try(logger, dsn, lambda conn: audit_delete(conn, AUDIT_MISSING_FQN, tid), "audit_delete(pending)")
        total_ok += 1

    def update_stats(conn):
        update_run_stats(conn, AUDIT_RUN_FQN, run_id, len(api_ids) + len(new_ids), total_missing)
        return True

    db_try(logger, dsn, update_stats, "update_run_stats", retries=txn_retries)

    logger.info(
        f"detail: Finalizado. OK={total_ok} (janela={total_window}, novos={total_new}), "
        f"Muito_grandes={total_big}, Falhas={total_missing}."
    )


if __name__ == "__main__":
    main()
