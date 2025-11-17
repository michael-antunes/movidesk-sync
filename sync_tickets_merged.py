import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")
BATCH = int(os.getenv("MERGED_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))
SOURCE_KEY = os.getenv("MERGED_SOURCE_KEY", "sourceId")
TARGET_KEY = os.getenv("MERGED_TARGET_KEY", "targetId")
DATE_KEY = os.getenv("MERGED_DATE_KEY", "mergedDate")

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/merged"})

def md_get(path_or_full, params=None, ok_404=False):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN
    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or []
    if ok_404 and r.status_code == 404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        retry = r.headers.get("retry-after")
        wait = int(retry) if isinstance(retry, str) and retry.isdigit() else 60
        time.sleep(wait)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or []
    r.raise_for_status()

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
        create table if not exists visualizacao_resolvidos.tickets_mesclados(
          ticket_id integer primary key,
          merged_into_id integer,
          merged_at timestamptz,
          situacao_mesclado text generated always as ('Sim') stored,
          raw_payload jsonb,
          imported_at timestamptz default now()
        )
        """
        )
        cur.execute(
            "create index if not exists ix_tk_merged_into on visualizacao_resolvidos.tickets_mesclados(merged_into_id)"
        )
    conn.commit()

def upsert_rows(conn, rows):
    if not rows:
        return
    sql = """
    insert into visualizacao_resolvidos.tickets_mesclados
      (ticket_id, merged_into_id, merged_at, raw_payload, imported_at)
    values %s
    on conflict (ticket_id) do update set
      merged_into_id = excluded.merged_into_id,
      merged_at      = coalesce(excluded.merged_at, visualizacao_resolvidos.tickets_mesclados.merged_at),
      raw_payload    = excluded.raw_payload,
      imported_at    = now()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()

def normalize_items(data):
    if not data:
        return [], False
    if isinstance(data, dict):
        items = data.get("items") or data.get("value") or []
        has_more = bool(data.get("hasMore"))
        return items, has_more
    if isinstance(data, list):
        return data, len(data) >= BATCH
    return [], False

def extract_row(it):
    src = it.get(SOURCE_KEY) or it.get("sourceId") or it.get("ticketId") or it.get("source") or it.get("fromId")
    dst = it.get(TARGET_KEY) or it.get("targetId") or it.get("mergedIntoId") or it.get("target") or it.get("toId")
    dt = it.get(DATE_KEY) or it.get("mergedDate") or it.get("performedAt") or it.get("date")
    try:
        if src is not None:
            src = int(src)
        if dst is not None:
            dst = int(dst)
    except Exception:
        return None
    if not src:
        return None
    return src, dst, dt, Json(it)

def sync_merged(conn):
    skip = 0
    total = 0
    while True:
        params = {
            "$top": BATCH,
            "$skip": skip,
            "$orderby": "mergedDate desc",
        }
        data = md_get("tickets/merged", params=params, ok_404=True)
        if data is None:
            break
        items, has_more = normalize_items(data)
        if not items:
            break
        rows = []
        for it in items:
            row = extract_row(it)
            if row:
                rows.append(row)
        upsert_rows(conn, rows)
        total += len(rows)
        if not has_more or len(items) < BATCH:
            break
        skip += len(items)
        time.sleep(THROTTLE)
    print(f"tickets_mesclados: {total} linhas sincronizadas.")

def main():
    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        sync_merged(conn)

if __name__ == "__main__":
    main()
