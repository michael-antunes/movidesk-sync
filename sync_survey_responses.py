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

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN nos secrets.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("tickets_satisfacao")

S = requests.Session()
S.headers.update({"Accept": "application/json"})


def req_json(url, params=None, timeout=90):
    while True:
        r = S.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("Throttle HTTP %s, aguardando %ss…", r.status_code, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json() if r.text else None


def iint(x):
    try:
        s = str(x).strip()
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def parse_dt(x):
    if not x:
        return None
    s = str(x).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def to_iso_z(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def merged_table_info(conn):
    with conn.cursor() as cur:
        cur.execute("select to_regclass('visualizacao_resolvidos.tickets_mesclados')")
        ok = cur.fetchone()[0] is not None
        if not ok:
            return None
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'visualizacao_resolvidos'
              and table_name = 'tickets_mesclados'
            """
        )
        cols = {r[0] for r in cur.fetchall()}
    src = "ticket_id" if "ticket_id" in cols else None
    dst_candidates = ["merged_into_id", "mergedIntoId", "merged_into", "mergedInto", "merged_ticket_id", "mergedTicketId"]
    dst = next((c for c in dst_candidates if c in cols), None)
    if not src or not dst:
        return None
    return {"schema": "visualizacao_resolvidos", "table": "tickets_mesclados", "src": src, "dst": dst}


def resolve_canonical_map(conn, ticket_ids):
    ids = sorted({int(x) for x in ticket_ids if str(x).isdigit()})
    if not ids:
        return {}
    mi = merged_table_info(conn)
    if not mi:
        return {i: i for i in ids}

    canonical = {i: i for i in ids}

    for _ in range(12):
        current = sorted({canonical[i] for i in canonical})
        if not current:
            break
        with conn.cursor() as cur:
            q = f"""
            select {mi["src"]}::bigint as src, {mi["dst"]}::bigint as dst
            from {mi["schema"]}.{mi["table"]}
            where {mi["src"]} = any(%s)
              and {mi["dst"]} is not null
            """
            cur.execute(q, (current,))
            rows = cur.fetchall()
        m = {int(a): int(b) for a, b in rows if a is not None and b is not None}
        if not m:
            break
        changed = False
        for orig in list(canonical.keys()):
            c = canonical[orig]
            if c in m and m[c] != c:
                canonical[orig] = m[c]
                changed = True
        if not changed:
            break

    return canonical


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_satisfacao")
        cur.execute(
            """
            create table if not exists visualizacao_satisfacao.tickets_satisfacao(
              ticket_id  bigint,
              raw        jsonb not null,
              updated_at timestamptz not null default now()
            )
            """
        )
        cur.execute("alter table visualizacao_satisfacao.tickets_satisfacao add column if not exists ticket_id bigint")
        cur.execute("alter table visualizacao_satisfacao.tickets_satisfacao add column if not exists raw jsonb")
        cur.execute("alter table visualizacao_satisfacao.tickets_satisfacao add column if not exists updated_at timestamptz")
        cur.execute("update visualizacao_satisfacao.tickets_satisfacao set updated_at = now() where updated_at is null")
        cur.execute("create index if not exists idx_tickets_satisfacao_ticket_id on visualizacao_satisfacao.tickets_satisfacao(ticket_id)")
    conn.commit()


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
        page = req_json(url, params=params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(THROTTLE)
    return items


def dedupe_latest_by_ticket(items, canonical_map):
    best = {}
    best_dt = {}

    for it in items:
        if not isinstance(it, dict):
            continue
        orig_tid = iint(it.get("ticketId"))
        if orig_tid is None:
            continue
        canon_tid = canonical_map.get(orig_tid, orig_tid)
        dt = parse_dt(it.get("responseDate")) or datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

        enriched = dict(it)
        enriched["originalTicketId"] = orig_tid
        enriched["canonicalTicketId"] = canon_tid

        if canon_tid not in best_dt or dt >= best_dt[canon_tid]:
            best_dt[canon_tid] = dt
            best[canon_tid] = enriched

    return best


def upsert_raw_by_replace(conn, best_map):
    if not best_map:
        return 0

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    values = [(int(tid), Json(raw), now_utc) for tid, raw in best_map.items() if tid is not None]
    if not values:
        return 0

    with conn.cursor() as cur:
        cur.execute("create temporary table tmp_satisfacao(ticket_id bigint primary key, raw jsonb not null, updated_at timestamptz not null) on commit drop")
        execute_values(cur, "insert into tmp_satisfacao(ticket_id, raw, updated_at) values %s", values, page_size=500)

        cur.execute(
            """
            delete from visualizacao_satisfacao.tickets_satisfacao t
            using tmp_satisfacao s
            where t.ticket_id = s.ticket_id
            """
        )

        cur.execute(
            """
            insert into visualizacao_satisfacao.tickets_satisfacao(ticket_id, raw, updated_at)
            select ticket_id, raw, updated_at
            from tmp_satisfacao
            """
        )

    conn.commit()
    return len(values)


def main():
    logger.info("Iniciando sync de tickets_satisfacao (somente ticket_id + raw).")

    force_backfill_days = os.getenv("FORCE_BACKFILL_DAYS")
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    back_days = int(force_backfill_days) if force_backfill_days and force_backfill_days.isdigit() else DAYS_BACK
    since_iso = to_iso_z(now_utc - datetime.timedelta(days=back_days))

    items = fetch_survey_responses(since_iso)
    logger.info("Survey retornou %s respostas desde %s", len(items), since_iso)

    orig_ids = sorted({iint(x.get("ticketId")) for x in items if isinstance(x, dict) and iint(x.get("ticketId")) is not None})

    with psycopg2.connect(NEON_DSN) as conn:
        ensure_table(conn)
        canonical_map = resolve_canonical_map(conn, orig_ids)
        best = dedupe_latest_by_ticket(items, canonical_map)
        n = upsert_raw_by_replace(conn, best)

    logger.info("Finalizado. tickets_unicos=%s | upsert_replace=%s", len(best), n)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Falha no sync_survey_responses")
        raise
