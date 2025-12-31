import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values, Json


API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
DEFAULT_SCHEMA = "visualizacao_resolvidos"
CONTROL_TABLE = "range_scan_control"
TARGET_TABLE = "tickets_excluidos"


def setup_logger():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger("tickets_excluidos")


def now_utc():
    return datetime.now(timezone.utc)


def pg_connect(dsn: str):
    return psycopg2.connect(
        dsn,
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "10")),
        application_name=os.getenv("PGAPPNAME", "movidesk-excluidos"),
        keepalives=1,
        keepalives_idle=int(os.getenv("PGKEEPALIVES_IDLE", "30")),
        keepalives_interval=int(os.getenv("PGKEEPALIVES_INTERVAL", "10")),
        keepalives_count=int(os.getenv("PGKEEPALIVES_COUNT", "5")),
    )


def req(session: requests.Session, url: str, params: dict, attempts: int, timeout: int, throttle: float):
    last_err = None
    for i in range(attempts):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 ** i, 30) + throttle)
                continue
            last_err = f"{r.status_code} {r.text}"
            break
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(min(2 ** i, 30) + throttle)
    raise RuntimeError(last_err or "request failed")


def ensure_tables(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {schema}")
        cur.execute(
            f"""
            create table if not exists {schema}.{TARGET_TABLE} (
              ticket_id bigint primary key,
              last_update timestamptz,
              raw jsonb not null,
              synced_at timestamptz not null default now()
            )
            """
        )
        cur.execute(f"create index if not exists idx_{TARGET_TABLE}_last_update on {schema}.{TARGET_TABLE}(last_update)")
        cur.execute(f"alter table {schema}.{CONTROL_TABLE} add column if not exists id_atual_excluido bigint")
        cur.execute(f"update {schema}.{CONTROL_TABLE} set id_atual_excluido = coalesce(id_atual_excluido, id_inicial)")
    conn.commit()


def read_control(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select ctid::text, id_inicial, id_final, id_atual_excluido
            from {schema}.{CONTROL_TABLE}
            order by created_at desc nulls last, updated_at desc nulls last
            limit 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"ctid": row[0], "id_inicial": row[1], "id_final": row[2], "id_atual_excluido": row[3]}


def update_ptr(conn, schema: str, ctid: str, new_ptr: int):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {schema}.{CONTROL_TABLE}
            set id_atual_excluido=%s, updated_at=now()
            where ctid::text=%s
            """,
            [new_ptr, ctid],
        )
    conn.commit()


def fetch_ticket(session: requests.Session, token: str, ticket_id: int, endpoint: str, attempts: int, timeout: int, throttle: float):
    url = f"{API_BASE}/{endpoint}"
    params = {
        "token": token,
        "includeDeletedItems": "true",
        "$filter": f"id eq {ticket_id}",
        "$top": 1,
        "$select": "id,protocol,subject,status,baseStatus,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,ownerTeam,lastUpdate,isDeleted,createdDate,origin,category,urgency,justification",
        "$expand": "clients($expand=organization)",
    }
    data = req(session, url, params, attempts, timeout, throttle)
    if isinstance(data, list) and data:
        return data[0]
    return None


def upsert_deleted(conn, schema: str, rows: list[dict]):
    if not rows:
        return 0
    values = []
    for t in rows:
        tid = t.get("id")
        if tid is None:
            continue
        lu = t.get("lastUpdate")
        values.append((int(tid), lu, Json(t), now_utc()))
    if not values:
        return 0
    sql = f"""
        insert into {schema}.{TARGET_TABLE} (ticket_id, last_update, raw, synced_at)
        values %s
        on conflict (ticket_id) do update set
          last_update=excluded.last_update,
          raw=excluded.raw,
          synced_at=excluded.synced_at
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def main():
    log = setup_logger()

    dsn = os.getenv("NEON_DSN")
    token = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
    if not dsn:
        raise SystemExit("NEON_DSN é obrigatório")
    if not token:
        raise SystemExit("MOVIDESK_TOKEN é obrigatório")

    schema = os.getenv("SCAN_SCHEMA", DEFAULT_SCHEMA)
    batch_size = int(os.getenv("EXCLUIDOS_BATCH_SIZE", "50"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
    timeout = int(os.getenv("MOVIDESK_TIMEOUT", "30"))
    attempts = int(os.getenv("MOVIDESK_ATTEMPTS", "6"))

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    with pg_connect(dsn) as conn:
        ensure_tables(conn, schema)
        control = read_control(conn, schema)
        if not control:
            raise SystemExit("range_scan_control não encontrado")

        id_inicial = int(control["id_inicial"])
        id_final = int(control["id_final"])
        ptr = int(control["id_atual_excluido"] or id_inicial)

        if ptr < id_final:
            log.info("scanner_excluidos: fim (ptr < id_final)")
            return

        to_id = max(id_final, ptr - batch_size + 1)

        deleted = []
        checked = 0
        for tid in range(ptr, to_id - 1, -1):
            t = fetch_ticket(session, token, tid, "tickets", attempts, timeout, throttle)
            if t is None:
                t = fetch_ticket(session, token, tid, "tickets/past", attempts, timeout, throttle)
            if t is not None and t.get("isDeleted") is True:
                deleted.append(t)
            checked += 1
            time.sleep(throttle)

        upserted = upsert_deleted(conn, schema, deleted)

        new_ptr = to_id - 1
        update_ptr(conn, schema, control["ctid"], new_ptr)

        log.info(f"scanner_excluidos: checked={checked} deleted_found={len(deleted)} upserted={upserted} ptr {ptr}->{new_ptr}")


if __name__ == "__main__":
    main()
