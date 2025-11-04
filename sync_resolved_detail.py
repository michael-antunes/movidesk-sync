import os, time, requests, psycopg2, psycopg2.extras, datetime

API_BASE="https://api.movidesk.com/public/v1"
API_TOKEN=os.environ["MOVIDESK_TOKEN"]
NEON_DSN=os.environ["NEON_DSN"]

http=requests.Session()
http.headers.update({"Accept":"application/json"})

def _req(url, params, timeout=90):
    while True:
        r=http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            retry=r.headers.get("retry-after")
            wait=int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code==404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def get_since(conn):
    with conn.cursor() as cur:
        cur.execute("select last_update from visualizacao_resolvidos.sync_control where name='tickets_resolvidos'")
        row=cur.fetchone()
    base=row[0] if row and row[0] else datetime.datetime.now(datetime.timezone.utc)
    return base-datetime.timedelta(hours=4)

def to_iso_z(dt):
    return dt.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def norm_ts(v):
    if v is None:
        return None
    s=str(v).strip()
    if s=="" or s=="0001-01-01T00:00:00Z":
        return None
    return s

def fetch_tickets(since_iso):
    url=f"{API_BASE}/tickets"
    top=int(os.getenv("MOVIDESK_PAGE_SIZE","500"))
    throttle=float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    skip=0
    select_fields="id,status,baseStatus,resolvedIn,closedIn,lastUpdate"
    expand="owner,clients($expand=organization)"
    filtro=f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso}"
    items=[]
    while True:
        page=_req(url,{"token":API_TOKEN,"$select":select_fields,"$expand":expand,"$filter":filtro,"$orderby":"lastUpdate asc","$top":top,"$skip":skip}) or []
        if not isinstance(page,list) or not page:
            break
        items.extend(page)
        if len(page)<top:
            break
        skip+=len(page)
        time.sleep(throttle)
    return items

def map_row(t):
    tid=t.get("id")
    if isinstance(tid,str) and tid.isdigit():
        tid=int(tid)
    owner=t.get("owner") or {}
    clients=t.get("clients") or []
    org={}
    if isinstance(clients,list) and clients:
        org=clients[0].get("organization") or {}
    rid=owner.get("id")
    rid_val=int(rid) if str(rid).isdigit() else None
    return {
        "ticket_id":tid,
        "status":t.get("baseStatus") or t.get("status"),
        "last_resolved_at":norm_ts(t.get("resolvedIn")),
        "last_closed_at":norm_ts(t.get("closedIn")),
        "responsible_id":rid_val,
        "responsible_name":owner.get("businessName"),
        "organization_id":str(org.get("id")) if org.get("id") is not None else None,
        "organization_name":org.get("businessName")
    }

UPSERT_SQL="""
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,responsible_id,responsible_name,organization_id,organization_name)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s)
on conflict (ticket_id) do update set
  status=excluded.status,
  last_resolved_at=excluded.last_resolved_at,
  last_closed_at=excluded.last_closed_at,
  responsible_id=excluded.responsible_id,
  responsible_name=excluded.responsible_name,
  organization_id=excluded.organization_id,
  organization_name=excluded.organization_name
"""

def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=300)
    conn.commit()
    return len(rows)

def update_control(conn):
    with conn.cursor() as cur:
        cur.execute("""
        insert into visualizacao_resolvidos.sync_control(name,last_update)
        values ('tickets_resolvidos', now())
        on conflict (name) do update set last_update=excluded.last_update
        """)
    conn.commit()

def main():
    conn=psycopg2.connect(NEON_DSN)
    try:
        since_dt=get_since(conn)
        since_iso=to_iso_z(since_dt)
        items=fetch_tickets(since_iso)
        rows=[map_row(t) for t in items if isinstance(t,dict)]
        n=upsert_rows(conn, rows)
        update_control(conn)
        print(f"tickets_resolvidos upsert: {n} desde {since_iso}")
    finally:
        conn.close()

if __name__=="__main__":
    main()
