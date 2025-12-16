import os
import time
import datetime
import logging

import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

PAGE_SIZE = max(1, min(100, int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))

SURVEY_TYPE = int(os.getenv("MOVIDESK_SURVEY_TYPE", "2"))
DAYS_BACK = int(os.getenv("MOVIDESK_SURVEY_DAYS", "120"))
OVERLAP_MIN = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN nos secrets.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("tickets_satisfacao")

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
            return {}
        r.raise_for_status()
        return r.json() if r.text else {}


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def to_iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_satisfacao")
        cur.execute(
            """
            create table if not exists visualizacao_satisfacao.tickets_satisfacao(
              id            text primary key,
              ticket_id      bigint,
              type           integer,
              response_date  timestamptz,
              raw            jsonb not null,
              updated_at     timestamptz not null default now()
            )
            """
        )
        cur.execute("create index if not exists idx_tickets_satisfacao_ticket on visualizacao_satisfacao.tickets_satisfacao(ticket_id)")
        cur.execute("create index if not exists idx_tickets_satisfacao_response_date on visualizacao_satisfacao.tickets_satisfacao(response_date)")
    conn.commit()


def get_since_from_db(conn):
    with conn.cursor() as cur:
        cur.execute("select max(response_date) from visualizacao_satisfacao.tickets_satisfacao")
        row = cur.fetchone()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    floor = now_utc - datetime.timedelta(days=DAYS_BACK)
    if row and row[0]:
        since = row[0] - datetime.timedelta(minutes=OVERLAP_MIN)
        return max(since, floor)
    return floor


def fetch_survey_responses(since_iso):
    url = f"{API_BASE}/survey/responses"
    starting_after = None
    items = []
    while True:
        params = {
            "token": API_TOKEN,
            "type": SURVEY_TYPE,
            "responseDateGreaterThan": since_iso,
            "limit": PAGE_SIZE,
        }
        if starting_after:
            params["startingAfter"] = starting_after
        page = req(url, params=params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(THROTTLE)
    return items


def upsert_raw(conn, items):
    if not items:
        return 0

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    values = []
    for it in items:
        if not isinstance(it, dict):
            continue
        rid = str(it.get("id") or "").strip()
        if not rid:
            continue
        values.append(
            (
                rid,
                iint(it.get("ticketId")),
                iint(it.get("type")),
                it.get("responseDate"),
                Json(it),
                now_utc,
            )
        )

    if not values:
        return 0

    sql = """
    insert into visualizacao_satisfacao.tickets_satisfacao
      (id, ticket_id, type, response_date, raw, updated_at)
    values %s
    on conflict (id) do update set
      ticket_id = excluded.ticket_id,
      type = excluded.type,
      response_date = excluded.response_date,
      raw = excluded.raw,
      updated_at = excluded.updated_at
    """

    with conn.cursor() as cur:
        cur.execute("set local synchronous_commit=off")
        execute_values(cur, sql, values, page_size=200)
    conn.commit()
    return len(values)


def main():
    logger.info("Iniciando sync de tickets_satisfacao (raw do /survey/responses).")

    with psycopg2.connect(NEON_DSN) as conn:
        ensure_table(conn)
        since_dt = get_since_from_db(conn)
        since_iso = to_iso_z(since_dt)

        items = fetch_survey_responses(since_iso)
        n = upsert_raw(conn, items)

    logger.info("Finalizado. DESDE=%s | upsert=%s", since_iso, n)


if __name__ == "__main__":
    main()
