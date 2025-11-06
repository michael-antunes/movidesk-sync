import os, time, requests, datetime, psycopg2, psycopg2.extras

API_BASE  = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
      r = http.get(url, params=params, timeout=timeout)
      if r.status_code in (429, 503):
        wait = int(r.headers.get("retry-after") or 60)
        time.sleep(wait); continue
      if r.status_code == 404:
        return {}
      r.raise_for_status()
      return r.json() if r.text else {}

def _norm_ts(x):
    if not x: return None
    s = str(x).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s

def _iint(x):
    try:
        s = str(x); return int(s) if s.isdigit() else None
    except Exception:
        return None

UPSERT_TICKET_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,responsible_id,responsible_name,organization_id,organization_name,origin,category,urgency,service_first_level,service_second_level,service_third_level)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s,%(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s)
on conflict (ticket_id) do update set
  status = excluded.status,
  last_resolved_at = excluded.last_resolved_at,
  last_closed_at = excluded.last_closed_at,
  responsible_id = excluded.responsible_id,
  responsible_name = excluded.responsible_name,
  organization_id = excluded.organization_id,
  organization_name = excluded.organization_name,
  origin = excluded.origin,
  category = excluded.category,
  urgency = excluded.urgency,
  service_first_level = excluded.service_first_level,
  service_second_level = excluded.service_second_level,
  service_third_level = excluded.service_third_level
"""

UPSERT_ACOES_SQL = """
insert into visualizacao_resolvidos.resolvidos_acoes(ticket_id, acoes)
values (%s, %s)
on conflict (ticket_id) do update set acoes = excluded.acoes
"""

UPSERT_DETAIL_SQL = """
insert into visualizacao_resolvidos.detail_control(ticket_id,last_update)
values (%s,%s)
on conflict (ticket_id) do update set last_update = greatest(excluded.last_update, visualizacao_resolvidos.detail_control.last_update)
"""

def map_ticket(t: dict):
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    org = (c0.get("organization") or {}) if isinstance(c0, dict) else {}
    s1 = t.get("serviceFirstLevel")
    s2 = t.get("serviceSecondLevel")
    s3 = t.get("serviceThirdLevel")
    tid = t.get("id")
    if isinstance(tid,str) and tid.isdigit(): tid = int(tid)
    return {
        "ticket_id": tid,
        "status": (t.get("baseStatus") or t.get("status") or None),
        "last_resolved_at": _norm_ts(t.get("resolvedIn")),
        "last_closed_at":   _norm_ts(t.get("closedIn")),
        "responsible_id": _iint(owner.get("id")),
        "responsible_name": owner.get("businessName"),
        "organization_id": (org.get("id") if isinstance(org, dict) else None),
        "organization_name": (org.get("businessName") if isinstance(org, dict) else None),
        "origin": t.get("origin") or t.get("originName"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": s3,
        "service_second_level": s2,
        "service_third_level": s1
    }

def fetch_full_ticket(ticket_id: int) -> dict:
    url = f"{API_BASE}/tickets/{ticket_id}"
    params = {"token": API_TOKEN, "$expand": "owner,clients($expand=organization),actions"}
    return _req(url, params) or {}

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    with psycopg2.connect(NEON_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("select distinct ticket_id from visualizacao_resolvidos.audit_recent_missing")
            ids = [r[0] for r in cur.fetchall()]
    if not ids:
        return
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    rows_ticket, rows_acoes, rows_detail = [], [], []
    for tid in ids:
        t = fetch_full_ticket(tid)
        if not isinstance(t, dict) or not t:
            continue
        rows_ticket.append(map_ticket(t))
        acoes = t.get("actions") if isinstance(t.get("actions"), list) else []
        rows_acoes.append((tid, psycopg2.extras.Json(acoes)))
        last_up = t.get("lastUpdate")
        rows_detail.append((tid, last_up))
        time.sleep(throttle)
    with psycopg2.connect(NEON_DSN) as conn:
        with conn.cursor() as cur:
            if rows_ticket:
                psycopg2.extras.execute_batch(cur, UPSERT_TICKET_SQL, rows_ticket, page_size=200)
            if rows_acoes:
                psycopg2.extras.execute_batch(cur, UPSERT_ACOES_SQL, rows_acoes, page_size=200)
            if rows_detail:
                psycopg2.extras.execute_batch(cur, UPSERT_DETAIL_SQL, rows_detail, page_size=200)
        conn.commit()

if __name__ == "__main__":
    main()
