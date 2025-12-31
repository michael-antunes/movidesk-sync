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
CONTROL_TABLE = os.getenv("CONTROL_TABLE", "range_scan_control")
TABLE_EXCLUIDOS = os.getenv("TABLE_EXCLUIDOS", "tickets_excluidos")

BATCH = int(os.getenv("EXCLUIDOS_BATCH", "50"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
TIMEOUT = int(os.getenv("MOVIDESK_TIMEOUT", "30"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "6"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync_tickets_excluidos")

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
        cur.execute(f'alter table {qname(SCHEMA, CONTROL_TABLE)} add column if not exists id_atual_excluido bigint')
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
    conn.commit()


def read_control(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select id_inicial, id_final, id_atual, id_atual_excluido
            from {qname(SCHEMA, CONTROL_TABLE)}
            limit 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("range_scan_control vazio")
        id_inicial, id_final, id_atual, id_atual_excluido = row
        return (
            int(id_inicial),
            int(id_final),
            int(id_atual) if id_atual is not None else None,
            int(id_atual_excluido) if id_atual_excluido is not None else None,
        )


def write_ptr(conn, new_ptr):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {qname(SCHEMA, CONTROL_TABLE)}
            set id_atual_excluido=%s
            """,
            (int(new_ptr),),
        )
    conn.commit()


def fetch_ticket(ticket_id, endpoint):
    url = f"{API_BASE}/{endpoint}"
    params = {
        "token": TOKEN,
        "includeDeletedItems": "true",
        "$filter": f"id eq {int(ticket_id)}",
        "$top": 1,
        "$select": "id,protocol,subject,status,baseStatus,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,ownerTeam,lastUpdate,isDeleted,createdDate,origin,category,urgency,justification",
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

        id_inicial, id_final, id_atual, id_atual_excluido = read_control(conn)

        ptr = id_atual_excluido if id_atual_excluido is not None else id_inicial
        if ptr is None and id_atual is not None:
            ptr = id_atual

        if ptr is None:
            raise RuntimeError("Ponteiro inicial não definido (id_inicial/id_atual_excluido/id_atual)")

        if ptr < id_final:
            log.info("Fim: id_atual_excluido=%s < id_final=%s", ptr, id_final)
            return

        to_id = max(id_final, ptr - BATCH + 1)

        encontrados = []
        checked = 0
        for tid in range(ptr, to_id - 1, -1):
            t = fetch_by_id(tid)
            if t is not None and t.get("isDeleted") is True:
                encontrados.append(t)
            checked += 1
            time.sleep(THROTTLE)

        up = upsert_excluidos(conn, encontrados)

        new_ptr = to_id - 1
        write_ptr(conn, new_ptr)

        log.info("OK: checked=%s deleted_found=%s upserted=%s id_atual_excluido=%s->%s", checked, len(encontrados), up, ptr, new_ptr)


if __name__ == "__main__":
    main()
