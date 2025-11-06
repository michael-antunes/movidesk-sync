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

def _extract_custom_value(ticket, custom_id):
    pools = []
    for k in ("customFieldValues", "customFields", "additionalFields", "custom_fields"):
        v = ticket.get(k)
        if isinstance(v, list):
            pools.extend(v)
    def match(d):
        for k in ("id", "customFieldId", "fieldId", "customfieldid"):
            if str(d.get(k, "")).strip() == str(custom_id):
                return True
        return False
    for it in pools:
        if isinstance(it, dict) and match(it):
            for vk in ("value", "currentValue", "text", "valueName", "name", "option"):
                if it.get(vk) not in (None, ""):
                    val = it.get(vk)
                    if isinstance(val, list):
                        return ", ".join([str(x) for x in val])
                    return str(val)
            return None
    return None

def fetch_custom_for_tickets(ticket_ids, custom_id):
    ids = [int(x) for x in ticket_ids if str(x).isdigit()]
    if not ids:
        return {}
    url = f"{API_BASE}/tickets"
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    chunk = max(1, min(20, int(os.getenv("MOVIDESK_TICKETS_CHUNK", "10"))))
    out = {}
    def fetch_batch(batch):
        params = {
            "token": API_TOKEN,
            "$select": "id",
            "$expand": "customFieldValues",
            "$filter": " or ".join([f"id eq {i}" for i in batch]),
            "$top": max(1, len(batch)),
            "$skip": 0
        }
        page = _req_list(url, params) or []
        for t in page:
            try:
                tid = int(t.get("id"))
            except Exception:
                continue
            out[tid] = _extract_custom_value(t, custom_id)
    def safe_fetch(batch):
        try:
            fetch_batch(batch)
        except requests.HTTPError as e:
            if getattr(e.response, "status_code", None) == 400 and len(batch) > 1:
                mid = len(batch) // 2
                safe_fetch(batch[:mid])
                safe_fetch(batch[mid:])
            elif getattr(e.response, "status_code", None) == 400 and len(batch) == 1:
                try:
                    fetch_batch(batch)
                except Exception:
                    pass
            else:
                raise
    for i in range(0, len(ids), chunk):
        safe_fetch(ids[i:i+chunk])
        time.sleep(throttle)
    return out

def ids_with_null_custom(conn, limit_ids):
    open_ids = []
    res_ids = []
    with conn.cursor() as cur:
        cur.execute("""
            select id::int
            from visualizacao_atual.movidesk_tickets_abertos
            where adicional_137641_avaliado_csat is null
            order by id desc
            limit %s
        """, (limit_ids,))
        open_ids = [r[0] for r in cur.fetchall()]
        cur.execute("""
            select ticket_id::int
            from visualizacao_resolvidos.tickets_resolvidos
            where adicional_137641_avaliado_csat is null
            order by ticket_id desc
            limit %s
        """, (limit_ids,))
        res_ids = [r[0] for r in cur.fetchall()]
    ids = set()
    ids.update([i for i in open_ids if i is not None])
    ids.update([i for i in res_ids if i is not None])
    return sorted(ids)

def update_custom_in_tables(conn, values_map):
    if not values_map:
        return 0, 0
    open_rows = [{"id": int(tid), "val": (val if val not in ("", None) else None)} for tid, val in values_map.items()]
    res_rows  = [{"id": int(tid), "val": (val if val not in ("", None) else None)} for tid, val in values_map.items()]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            update visualizacao_atual.movidesk_tickets_abertos
            set adicional_137641_avaliado_csat = %(val)s
            where id = %(id)s
        """, open_rows, page_size=500)
        n1 = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        psycopg2.extras.execute_batch(cur, """
            update visualizacao_resolvidos.tickets_resolvidos
            set adicional_137641_avaliado_csat = %(val)s
            where ticket_id = %(id)s
        """, res_rows, page_size=500)
        n2 = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.commit()
    return n1, n2

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    days_back = int(os.getenv("MOVIDESK_SURVEY_DAYS", "120"))
    overlap_minutes = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))
    force_backfill_days = os.getenv("FORCE_BACKFILL_DAYS")
    custom_id = int(os.getenv("MOVIDESK_CUSTOM_ID", "137641"))
    limit_ids = int(os.getenv("MOVIDESK_LIMIT_IDS", "1500"))

    conn = psycopg2.connect(NEON_DSN)
    try:
        if force_backfill_days and force_backfill_days.isdigit():
            since_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(force_backfill_days))
        else:
            since_dt = get_since_from_db(conn, days_back, overlap_minutes)
        since_iso = to_iso_z(since_dt)

        resp = fetch_smiley_responses(since_iso)
        rows = [map_row(x) for x in resp if isinstance(x, dict)]
        n_resp = upsert_rows(conn, rows)

        tids_from_resp = {r["ticket_id"] for r in rows if r.get("ticket_id") is not None}
        backlog_ids = ids_with_null_custom(conn, limit_ids)
        all_ids = sorted({int(x) for x in tids_from_resp.union(backlog_ids)})

        n1 = n2 = 0
        if all_ids:
            values = fetch_custom_for_tickets(all_ids, custom_id)
            n1, n2 = update_custom_in_tables(conn, values)

        print(f"DESDE {since_iso} | UPSERT respostas: {n_resp} | Atualizado adicional 137641 -> abertos:{n1} resolvidos:{n2}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
