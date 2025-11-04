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
            return []
        r.raise_for_status()
        return r.json() if r.text else []

def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)

def _as_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def _iso_z(dt):
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

def _get_since(conn, job_name):
    with conn.cursor() as cur:
        cur.execute("select last_update from visualizacao_resolvidos.sync_control where name=%s", (job_name,))
        row = cur.fetchone()
    base = _as_utc(row[0]) if row and row[0] else None
    if not base:
        days = int(os.getenv("MOVIDESK_INDEX_DAYS", "180"))
        base = _utc_now() - datetime.timedelta(days=days)
    since = base - datetime.timedelta(hours=4)
    return since

def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None

def norm_ts(x):
    if not x:
        return None
    s = str(x).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s

def count_public_actions(t):
    acts = t.get("actions") or []
    return sum(1 for a in acts if isinstance(a, dict) and a.get("isPublic") is True)

def get_aberto_via(cf_list):
    if not isinstance(cf_list, list):
        return None
    for cf in cf_list:
        fid = cf.get("id") or cf.get("fieldId")
        if str(fid) == "184387":
            val = cf.get("value") or cf.get("currentValue") or cf.get("values")
            if isinstance(val, list):
                out = []
                for v in val:
                    if isinstance(v, dict):
                        out.append(str(v.get("label") or v.get("value") or v.get("name") or ""))
                    else:
                        out.append(str(v))
                return ", ".join([x for x in out if x])
            return str(val) if val is not None else None
    return None

def fetch_details(since_iso):
    url = f"{API_BASE}/tickets"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
    skip = 0
    select_fields = ",".join([
        "id","status","baseStatus","resolvedIn","closedIn","lastUpdate",
        "category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand_options = [
        "owner,clients($expand=organization),actions($select=isPublic;$top=2000),customFields",
        "owner,clients($expand=organization),actions($select=isPublic),customFields",
        "owner,clients($expand=organization)"
    ]
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge " + since_iso
    expand_idx = 0
    items = []
    while True:
        try:
            page = _req(url, {
                "token": API_TOKEN,
                "$select": select_fields,
                "$expand": expand_options[expand_idx],
                "$filter": filtro,
                "$orderby": "lastUpdate asc",
                "$top": top,
                "$skip": skip
            }) or []
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400 and expand_idx < len(expand_options) - 1:
                expand_idx += 1
                continue
            raise
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < top:
            break
        skip += len(page)
        time.sleep(throttle)
    return items

def map_row(t):
    tid = t.get("id")
    if isinstance(tid, str) and tid.isdigit():
        tid = int(tid)
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    org = c0.get("organization") or {}
    cf = t.get("customFields") or []
    return {
        "ticket_id": tid,
        "status": (t.get("baseStatus") or t.get("status") or None),
        "last_resolved_at": norm_ts(t.get("resolvedIn")),
        "last_closed_at": norm_ts(t.get("closedIn")),
        "responsible_id": iint(owner.get("id")),
        "responsible_name": owner.get("businessName"),
        "organization_id": org.get("id"),
        "organization_name": org.get("businessName"),
        "public_actions_count": int(count_public_actions(t)),
        "aberto_via_184387": get_aberto_via(cf),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel")
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,responsible_id,responsible_name,organization_id,organization_name,public_actions_count,aberto_via_184387,category,urgency,service_first_level,service_second_level,service_third_level)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(responsible_id)s,%(responsible_name)s,%(organization_id)s,%(organization_name)s,%(public_actions_count)s,%(aberto_via_184387)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s)
on conflict (ticket_id) do update set
  status = excluded.status,
  last_resolved_at = excluded.last_resolved_at,
  last_closed_at = excluded.last_closed_at,
  responsible_id = excluded.responsible_id,
  responsible_name = excluded.responsible_name,
  organization_id = excluded.organization_id,
  organization_name = excluded.organization_name,
  public_actions_count = excluded.public_actions_count,
  aberto_via_184387 = excluded.aberto_via_184387,
  category = excluded.category,
  urgency = excluded.urgency,
  service_first_level = excluded.service_first_level,
  service_second_level = excluded.service_second_level,
  service_third_level = excluded.service_third_level
"""

def upsert_details(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=300)
    conn.commit()
    return len(rows)

def mark_job_end(conn, job_name):
    with conn.cursor() as cur:
        cur.execute("update visualizacao_resolvidos.sync_control set last_update = now() where name=%s", (job_name,))
        if cur.rowcount == 0:
            cur.execute("insert into visualizacao_resolvidos.sync_control(name,last_update) values(%s, now())", (job_name,))
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    with psycopg2.connect(NEON_DSN) as conn:
        since_dt = _get_since(conn, "tickets_resolvidos")
        since_iso = _iso_z(since_dt)
        items = fetch_details(since_iso)
        rows = [map_row(t) for t in items if isinstance(t, dict) and str(t.get("id","")).isdigit()]
        upsert_details(conn, rows)
        mark_job_end(conn, "tickets_resolvidos")

if __name__ == "__main__":
    main()
