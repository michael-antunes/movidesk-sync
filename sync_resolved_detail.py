import os, time, datetime, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if str(ra).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(open("sql/01_resolvidos_schema.sql","r",encoding="utf-8").read())
    conn.commit()

def pick_ids(conn, limit):
    with conn.cursor() as cur:
        cur.execute("""
        select ticket_id
        from visualizacao_resolvidos.detail_control
        where need_detail = true or detail_last_run_at is null
        order by coalesce(last_update, now()) desc
        limit %s
        """, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]

def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None

def pick_org(clients):
    if not isinstance(clients, list) or not clients:
        return None, None
    c0 = clients[0] or {}
    org = c0.get("organization") or {}
    return org.get("id"), org.get("businessName")

def slug(s):
    import re
    return re.sub(r"[^a-z0-9]+","_",str(s or "").lower()).strip("_")

def extract_custom(cfs):
    out = {}
    if isinstance(cfs, list):
        for cf in cfs:
            fid = cf.get("id")
            name = cf.get("name")
            key = f"adicional_{fid}_{slug(name)}" if fid else None
            if key:
                out[key] = cf.get("value")
    return out

def fetch_detail(ticket_id):
    url = f"{API_BASE}/tickets"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization),customFields"
    filtro = f"id eq {ticket_id}"
    data = req(url, {
        "token": API_TOKEN,
        "$select": select_fields,
        "$expand": expand,
        "$filter": filtro,
        "$top": 1
    }) or []
    return data[0] if data else {}

UPSERT_TK = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 responsible_id,responsible_name,organization_id,organization_name,origin,category,urgency,
 service_first_level,service_second_level,service_third_level,adicional_137641_avaliado_csat)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s,%(origin)s,%(category)s,%(urgency)s,
 %(service_first_level)s,%(service_second_level)s,%(service_third_level)s,%(adicional_137641_avaliado_csat)s)
on conflict (ticket_id) do update set
  status=excluded.status,
  last_resolved_at=excluded.last_resolved_at,
  last_closed_at=excluded.last_closed_at,
  last_cancelled_at=excluded.last_cancelled_at,
  last_update=excluded.last_update,
  responsible_id=excluded.responsible_id,
  responsible_name=excluded.responsible_name,
  organization_id=excluded.organization_id,
  organization_name=excluded.organization_name,
  origin=excluded.origin,
  category=excluded.category,
  urgency=excluded.urgency,
  service_first_level=excluded.service_first_level,
  service_second_level=excluded.service_second_level,
  service_third_level=excluded.service_third_level,
  adicional_137641_avaliado_csat=excluded.adicional_137641_avaliado_csat
"""

def map_row(t):
    owner = t.get("owner") or {}
    org_id, org_name = pick_org(t.get("clients") or [])
    cfs = extract_custom(t.get("customFields") or [])
    return {
        "ticket_id": iint(t.get("id")),
        "status": t.get("status"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at": t.get("closedIn"),
        "last_cancelled_at": t.get("canceledIn"),
        "last_update": t.get("lastUpdate"),
        "responsible_id": iint(owner.get("id")),
        "responsible_name": owner.get("businessName"),
        "organization_id": org_id,
        "organization_name": org_name,
        "origin": str(t.get("origin") if t.get("origin") is not None else ""),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "adicional_137641_avaliado_csat": cfs.get("adicional_137641_avaliado_csat"),
    }

def upsert_detail(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_TK, rows, page_size=200)
    conn.commit()
    return len(rows)

def mark_done(conn, ids):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
        update visualizacao_resolvidos.detail_control
           set need_detail=false, detail_last_run_at=now(), tries=0
         where ticket_id = any(%s)
        """, (ids,))
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN")
    conn = psycopg2.connect(NEON_DSN)
    try:
        ensure_schema(conn)
        limit = int(os.getenv("MOVIDESK_DETAIL_LIMIT","2000"))
        throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
        ids = pick_ids(conn, limit)
        rows, done = [], []
        for tid in ids:
            t = fetch_detail(tid)
            if t:
                rows.append(map_row(t))
                done.append(tid)
            time.sleep(throttle)
        upsert_detail(conn, rows)
        mark_done(conn, done)
        with conn.cursor() as cur:
            cur.execute("""
              insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
              values('default',now(),now())
              on conflict (name) do update set last_update=excluded.last_update,last_detail_run_at=excluded.last_detail_run_at
            """)
        conn.commit()
        print(f"rows={len(rows)} done={len(done)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
