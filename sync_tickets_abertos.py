import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

RUN_DDL = os.getenv("RUN_DDL", "0") == "1"
PG_SYNC_COMMIT_OFF = os.getenv("PG_SYNC_COMMIT_OFF", "1") == "1"

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tickets_abertos_index")

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def req(url, params=None, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("Throttle HTTP %s, aguardando %ss…", r.status_code, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def norm_ts(x):
    if not x:
        return None
    s = str(x).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s


def set_fast_commit(cur):
    if PG_SYNC_COMMIT_OFF:
        cur.execute("set local synchronous_commit=off")


def fetch_open_ticket_min():
    url = f"{API_BASE}/tickets"
    skip = 0
    fil = "(baseStatus eq 'New' or baseStatus eq 'InAttendance' or baseStatus eq 'Stopped')"
    sel = "id,baseStatus,lastUpdate"

    items = []
    while True:
        params = {
            "token": TOKEN,
            "$select": sel,
            "$filter": fil,
            "$orderby": "lastUpdate asc",
            "$top": PAGE_SIZE,
            "$skip": skip,
        }

        page = req(url, params=params) or []
        if not page:
            break

        items.extend(page)
        if len(page) < PAGE_SIZE:
            break

        skip += len(page)
        time.sleep(THROTTLE)

    return items


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_atual")
        cur.execute(
            """
            create table if not exists visualizacao_atual.tickets_abertos(
              ticket_id       bigint primary key,
              raw             jsonb,
              updated_at      timestamptz default now(),
              raw_last_update timestamptz,
              last_update     timestamptz,
              base_status     text
            )
            """
        )
        cur.execute(
            """
            alter table visualizacao_atual.tickets_abertos
              add column if not exists subject                       varchar(350),
              add column if not exists type                          smallint,
              add column if not exists status                        varchar(128),
              add column if not exists owner_team                    varchar(128),
              add column if not exists service_first_level           varchar(102),
              add column if not exists created_date                  timestamptz,
              add column if not exists contagem                      integer default 1,
              add column if not exists responsavel                   varchar(255),
              add column if not exists empresa_cod_ref_adicional     varchar(64),
              add column if not exists agent_id                      bigint,
              add column if not exists empresa_id                    text,
              add column if not exists empresa_nome                  text,
              add column if not exists adicional_137641_avaliado_csat text,
              add column if not exists origin                        smallint,
              add column if not exists reaberturas                   integer default 0,
              add column if not exists service_second_level          text,
              add column if not exists service_third_level           text
            """
        )
    conn.commit()


def upsert_min(conn, items):
    now_utc = datetime.now(timezone.utc)
    values = []
    for t in items:
        if not isinstance(t, dict):
            continue
        tid = iint(t.get("id"))
        if tid is None:
            continue
        values.append(
            (
                tid,
                t.get("baseStatus"),
                norm_ts(t.get("lastUpdate")),
                now_utc,
            )
        )

    if not values:
        return 0

    sql = """
        insert into visualizacao_atual.tickets_abertos (
            ticket_id,
            base_status,
            last_update,
            updated_at
        ) values %s
        on conflict (ticket_id) do update set
            base_status = excluded.base_status,
            last_update = excluded.last_update,
            updated_at  = excluded.updated_at
        where visualizacao_atual.tickets_abertos.last_update is distinct from excluded.last_update
           or visualizacao_atual.tickets_abertos.base_status is distinct from excluded.base_status
    """

    with conn.cursor() as cur:
        set_fast_commit(cur)
        execute_values(cur, sql, values, page_size=500)

    conn.commit()
    return len(values)


def cleanup_not_open(conn, open_ids):
    if not open_ids:
        with conn.cursor() as cur:
            set_fast_commit(cur)
            cur.execute("delete from visualizacao_atual.tickets_abertos")
            removed = cur.rowcount
        conn.commit()
        return removed

    unique_ids = sorted({i for i in open_ids if i is not None})
    if not unique_ids:
        return 0

    with conn.cursor() as cur:
        set_fast_commit(cur)
        cur.execute("create temporary table tmp_open_ids(ticket_id bigint primary key) on commit drop")
        execute_values(
            cur,
            "insert into tmp_open_ids(ticket_id) values %s",
            [(i,) for i in unique_ids],
            page_size=2000,
        )
        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             where not exists (
               select 1 from tmp_open_ids t
                where t.ticket_id = ta.ticket_id
             )
            """
        )
        removed = cur.rowcount

    conn.commit()
    return removed


def cleanup_merged(conn):
    with conn.cursor() as cur:
        set_fast_commit(cur)
        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             using visualizacao_resolvidos.tickets_mesclados tm
             where ta.ticket_id = tm.ticket_id
            """
        )
        removed = cur.rowcount
    conn.commit()
    return removed


def main():
    logger.info("Iniciando sync de tickets abertos (index por ID).")

    items = fetch_open_ticket_min()
    open_ids = [iint(t.get("id")) for t in items if isinstance(t, dict)]

    with psycopg2.connect(DSN) as conn:
        if RUN_DDL:
            ensure_table(conn)

        up = upsert_min(conn, items)
        rem_closed = cleanup_not_open(conn, open_ids)
        rem_merged = cleanup_merged(conn)

    logger.info(
        "Finalizado. upsert=%s removidos_fechados=%s removidos_mesclados=%s total_api=%s",
        up,
        rem_closed,
        rem_merged,
        len(items),
    )


if __name__ == "__main__":
    main()
