import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values, Json


API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_EXCLUIDOS = os.getenv("TABLE_EXCLUIDOS", "tickets_excluidos")
CONTROL_TABLE = os.getenv("CONTROL_TABLE", "tickets_excluidos_scan_control")

BATCH = int(os.getenv("EXCLUIDOS_BATCH", "100"))
STOP_AT = int(os.getenv("EXCLUIDOS_STOP_AT", "1"))

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
TIMEOUT = int(os.getenv("MOVIDESK_TIMEOUT", "30"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "6"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync_tickets_excluidos_range")

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def now_utc():
    return datetime.now(timezone.utc)


def qname(schema, table):
    return f'"{schema}"."{table}"'


def parse_dt(s):
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    s = str(s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def req(url, params):
    last_err = None
    for i in range(ATTEMPTS):
        try:
            r = http.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json() if r.text else []
            if r.status_code == 404:
                return []
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2**i, 30) + THROTTLE)
                continue
            last_err = f"{r.status_code} {r.text}"
            break
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(min(2**i, 30) + THROTTLE)
    raise RuntimeError(last_err or "request failed")


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f'create schema if not exists "{SCHEMA}"')

        cur.execute(
            f"""
            create table if not exists {qname(SCHEMA, TABLE_EXCLUIDOS)} (
              ticket_id bigint primary key,
              date_excluido timestamptz,
              raw jsonb not null
            )
            """
        )
        cur.execute(f'alter table {qname(SCHEMA, TABLE_EXCLUIDOS)} add column if not exists date_excluido timestamptz')
        cur.execute(f'alter table {qname(SCHEMA, TABLE_EXCLUIDOS)} add column if not exists raw jsonb')
        cur.execute(
            f"create index if not exists idx_{TABLE_EXCLUIDOS}_date_excluido on {qname(SCHEMA, TABLE_EXCLUIDOS)} (date_excluido)"
        )

        cur.execute(
            f"""
            create table if not exists {qname(SCHEMA, CONTROL_TABLE)} (
              id int primary key,
              id_started bigint,
              id_ptr bigint,
              updated_at timestamptz
            )
            """
        )
        cur.execute(
            f"""
            insert into {qname(SCHEMA, CONTROL_TABLE)} (id)
            values (1)
            on conflict (id) do nothing
            """
        )
    conn.commit()


def read_control(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select id_started, id_ptr
            from {qname(SCHEMA, CONTROL_TABLE)}
            where id=1
            """
        )
        row = cur.fetchone()
        if not row:
            return None, None
        s, p = row
        return (int(s) if s is not None else None), (int(p) if p is not None else None)


def write_control(conn, id_started, id_ptr):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {qname(SCHEMA, CONTROL_TABLE)}
            set id_started=%s, id_ptr=%s, updated_at=now()
            where id=1
            """,
            (int(id_started) if id_started is not None else None, int(id_ptr) if id_ptr is not None else None),
        )
    conn.commit()


def fetch_max_ticket_id():
    order_bys = ["id desc", "createdDate desc", "lastUpdate desc"]
    endpoints = ["tickets", "tickets/past"]
    for ep in endpoints:
        url = f"{API_BASE}/{ep}"
        for ob in order_bys:
            params = {
                "token": TOKEN,
                "includeDeletedItems": "true",
                "$top": 1,
                "$select": "id",
                "$orderby": ob,
            }
            try:
                data = req(url, params)
            except Exception:
                continue
            if isinstance(data, list) and data:
                tid = data[0].get("id")
                if tid is not None:
                    return int(tid)
    raise RuntimeError("Não foi possível obter o maior ticket_id pela API")


def fetch_ticket(ticket_id, endpoint):
    url = f"{API_BASE}/{endpoint}"
    params = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$filter": f"id eq {int(ticket_id)}",
        "$top": 1,
        "$select": "id,protocol,subject,status,baseStatus,isDeleted,createdDate,origin,category,urgency,justification,lastUpdate",
        "$expand": "clients($expand=organization)",
    }
    data = req(url, params)
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_by_id(ticket_id):
    t = fetch_ticket(ticket_id, "tickets")
    if t is not None:
        return t
    return fetch_ticket(ticket_id, "tickets/past")


def upsert_excluidos(conn, items):
    if not items:
        return 0
    values = []
    for t in items:
        tid = t.get("id")
        if tid is None:
            continue
        dt = parse_dt(t.get("lastUpdate")) or now_utc()
        values.append((int(tid), dt, Json(t)))
    if not values:
        return 0
    sql = f"""
        insert into {qname(SCHEMA, TABLE_EXCLUIDOS)} (ticket_id, date_excluido, raw)
        values %s
        on conflict (ticket_id) do update set
          date_excluido=excluded.date_excluido,
          raw=excluded.raw
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(values)


def main():
    if not DSN:
        raise RuntimeError("NEON_DSN ausente")
    if not TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN ausente")

    with psycopg2.connect(DSN) as conn:
        ensure_tables(conn)

        id_started, ptr = read_control(conn)
        if ptr is None:
            max_id = fetch_max_ticket_id()
            id_started = max_id
            ptr = max_id
            write_control(conn, id_started, ptr)

        if ptr <= STOP_AT:
            log.info("Fim: id_ptr=%s <= stop_at=%s", ptr, STOP_AT)
            return

        to_id = max(STOP_AT, ptr - BATCH + 1)

        encontrados = []
        checked = 0
        deleted_found = 0
        not_found = 0

        for tid in range(ptr, to_id - 1, -1):
            t = fetch_by_id(tid)
            if t is not None and t.get("isDeleted") is True:
                encontrados.append(t)
                deleted_found += 1
            elif t is None:
                encontrados.append({"id": int(tid), "api_not_found": True, "checkedAt": now_utc().isoformat()})
                not_found += 1
            checked += 1
            time.sleep(THROTTLE)

        up = upsert_excluidos(conn, encontrados)

        new_ptr = to_id - 1
        write_control(conn, id_started, new_ptr)

        log.info(
            "OK: checked=%s deleted_found=%s not_found=%s upserted=%s id_ptr=%s->%s started_at=%s",
            checked,
            deleted_found,
            not_found,
            up,
            ptr,
            new_ptr,
            id_started,
        )


if __name__ == "__main__":
    main()
