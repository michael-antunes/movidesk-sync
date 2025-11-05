import os
import time
import datetime
import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json() if r.text else {}

def _req_list(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def iint(x):
    try:
        return int(str(x))
    except Exception:
        return None

def to_iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")

def get_since_from_db(conn, days_back, overlap_minutes):
    with conn.cursor() as cur:
        cur.execute("select max(response_date) from visualizacao_satisfacao.movidesk_pesquisa_satisfacao_respostas")
        row = cur.fetchone()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if row and row[0]:
        since = row[0] - datetime.timedelta(minutes=overlap_minutes)
        floor = now_utc - datetime.timedelta(days=days_back)
        return max(since, floor)
    return now_utc - datetime.timedelta(days=days_back)

def fetch_smiley_responses(since_iso):
    url = f"{API_BASE}/survey/responses"
    limit = max(1, min(100, int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    starting_after = None
    items = []
    while True:
        params = {"token": API_TOKEN, "type": 2, "responseDateGreaterThan": since_iso, "limit": limit}
        if starting_after:
            params["startingAfter"] = starting_after
        page = _req(url, params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(throttle)
    return items

def fetch_ticket_status_map(ticket_ids):
    ids = [str(i) for i in ticket_ids if str(i).isdigit()]
    if not ids:
        return {}
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_TICKETS_TOP", "200"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    chunk = max(1, min(80, int(os.getenv("MOVIDESK_TICKETS_CHUNK", "50"))))
    out = {}
    for i in range(0, len(ids), chunk):
        batch = ids[i:i+chunk]
        filtro = " or ".join([f"id eq {x}" for x in batch])
        skip = 0
        while True:
            page = _req_list(url, {"token": API_TOKEN, "$select": "id,baseStatus", "$filter": filtro, "$top": top, "$skip": skip}) or []
            if not page:
                break
            for t in page:
                tid = t.get("id")
                if isinstance(tid, str) and tid.isdigit():
                    tid = int(tid)
                out[tid] = t.get("baseStatus")
            if len(page) < top:
                break
            skip += len(page)
            time.sleep(throttle)
    return out

def map_row(r):
    return {
        "id": str(r.get("id") or ""),
        "ticket_id": iint(r.get("ticketId")),
        "type": iint(r.get("type")),
        "question_id": str(r.get("questionId") or ""),
        "client_id": str(r.get("clientId") or ""),
        "response_date": r.get("responseDate"),
        "value": iint(r.get("value")),
        "base_status": None,
    }

UPSERT_SQL = """
insert into visualizacao_satisfacao.movidesk_pesquisa_satisfacao_respostas
(id,ticket_id,type,question_id,client_id,response_date,value,base_status)
values (%(id)s,%(ticket_id)s,%(type)s,%(question_id)s,%(client_id)s,%(response_date)s,%(value)s,%(base_status)s)
on conflict (id) do update set
  ticket_id = excluded.ticket_id,
  type = excluded.type,
  question_id = excluded.question_id,
  client_id = excluded.client_id,
  response_date = excluded.response_date,
  value = excluded.value,
  base_status = excluded.base_status
"""

def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    days_back = int(os.getenv("MOVIDESK_SURVEY_DAYS", "120"))
    overlap_minutes = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))
    force_backfill_days = os.getenv("FORCE_BACKFILL_DAYS")
    conn = psycopg2.connect(NEON_DSN)
    try:
        if force_backfill_days and force_backfill_days.isdigit():
            since_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(force_backfill_days))
        else:
            since_dt = get_since_from_db(conn, days_back, overlap_minutes)
        since_iso = to_iso_z(since_dt)
        resp = fetch_smiley_responses(since_iso)
        rows = [map_row(x) for x in resp if isinstance(x, dict)]
        tids = [r["ticket_id"] for r in rows if r.get("ticket_id") is not None]
        status_map = fetch_ticket_status_map(sorted(set(tids)))
        for r in rows:
            if r["ticket_id"] is not None:
                r["base_status"] = status_map.get(r["ticket_id"])
        n = upsert_rows(conn, rows)
        print(f"DESDE {since_iso} | UPSERT: {n} respostas.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
