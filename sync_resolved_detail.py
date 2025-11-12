import os, time, json, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

TOP = int(os.getenv("PAGES_UPSERT", "7")) * 100
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.5"))

UPSERT = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,origin,category,urgency,service_first_level,service_second_level,service_third_level,owner_id,owner_name,organization_id,organization_name)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
on conflict (ticket_id) do update set
 status=excluded.status,
 last_resolved_at=excluded.last_resolved_at,
 last_closed_at=excluded.last_closed_at,
 last_cancelled_at=excluded.last_cancelled_at,
 last_update=excluded.last_update,
 origin=excluded.origin,
 category=excluded.category,
 urgency=excluded.urgency,
 service_first_level=excluded.service_first_level,
 service_second_level=excluded.service_second_level,
 service_third_level=excluded.service_third_level,
 owner_id=excluded.owner_id,
 owner_name=excluded.owner_name,
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = "select coalesce(max(last_detail_run_at), timestamp 'epoch') from visualizacao_resolvidos.sync_control where name='default'"

def conn():
    return psycopg2.connect(NEON_DSN)

def req(url, params, retries=4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.5*(i+1))
            continue
        r.raise_for_status()
    r.raise_for_status()

def iso(dt):
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z","+00:00")).astimezone(timezone.utc)
    except:
        return None

def fetch_pages(since_iso):
    url = "https://api.movidesk.com/public/v1/tickets"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso
    skip = 0
    total = 0
    while True:
        page = req(url, {
            "token":API_TOKEN,
            "$select":select_fields,
            "$expand":expand,
            "$filter":filtro,
            "$orderby":"lastUpdate asc",
            "$top": min(100, TOP - total),
            "$skip": skip
        }) or []
        if not page:
            break
        yield page
        got = len(page)
        total += got
        skip += got
        if total >= TOP or got < 100:
            break
        time.sleep(THROTTLE)

def row_from_ticket(t):
    owner_id = None
    owner_name = None
    try:
        o = t.get("owner") or {}
        owner_id = o.get("id")
        owner_name = o.get("businessName") or o.get("fullName")
    except:
        pass
    org_id = None
    org_name = None
    try:
        cl = (t.get("clients") or [])
        if cl:
            org = (cl[0].get("organization") or {})
            org_id = org.get("id")
            org_name = org.get("businessName") or org.get("fullName")
    except:
        pass
    return (
        t.get("id"),
        t.get("status"),
        iso(t.get("resolvedIn")),
        iso(t.get("closedIn")),
        iso(t.get("canceledIn")),
        iso(t.get("lastUpdate")),
        t.get("origin"),
        t.get("category"),
        t.get("urgency"),
        t.get("serviceFirstLevel"),
        t.get("serviceSecondLevel"),
        t.get("serviceThirdLevel"),
        owner_id,
        owner_name,
        org_id,
        org_name
    )

def main():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(GET_LASTRUN)
            since = cur.fetchone()[0]
    if since == datetime(1970,1,1,tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")
    rows = []
    for page in fetch_pages(since_iso):
        for t in page:
            rows.append(row_from_ticket(t))
    if rows:
        with conn() as c:
            with c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=200)
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
