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

def iint(x):
    try:
        return int(str(x))
    except Exception:
        return None

def fetch_smiley_responses():
    url = f"{API_BASE}/survey/responses"
    limit = max(1, min(100, int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    days_back = int(os.getenv("MOVIDESK_SURVEY_DAYS", "120"))
    start_dt = (datetime.datetime.utcnow() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    starting_after = None
    items = []
    while True:
        params = {
            "token": API_TOKEN,
            "type": 2,
            "responseDateGreaterThan": start_dt,
            "limit": limit
        }
        if starting_after:
            params["startingAfter"] = starting_after
        page = _req(url, params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        has_more = bool(page.get("hasMore"))
        if not page_items or not has_more:
            break
        starting_after = page_items[-1].get("id")
        time.sleep(throttle)
    return items

def map_row(r):
    return {
        "id": str(r.get("id") or ""),
        "ticket_id": iint(r.get("ticketId")),
        "type": iint(r.get("type")),
        "question_id": str(r.get("questionId") or ""),
        "client_id": str(r.get("clientId") or ""),
        "response_date": r.get("responseDate"),
        "value": iint(r.get("value")),
    }

UPSERT_SQL = """
insert into visualizacao_satisfacao.movidesk_pesquisa_satisfacao_respostas
(id,ticket_id,type,question_id,client_id,response_date,value)
values (%(id)s,%(ticket_id)s,%(type)s,%(question_id)s,%(client_id)s,%(response_date)s,%(value)s)
on conflict (id) do update set
  ticket_id = excluded.ticket_id,
  type = excluded.type,
  question_id = excluded.question_id,
  client_id = excluded.client_id,
  response_date = excluded.response_date,
  value = excluded.value
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
    resp = fetch_smiley_responses()
    rows = [map_row(x) for x in resp if isinstance(x, dict)]
    conn = psycopg2.connect(NEON_DSN)
    try:
        n = upsert_rows(conn, rows)
        print(f"UPSERT: {n} respostas.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
