import os
import time
import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
TOP = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def req(url, params=None, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []


def fetch_open():
    url = f"{API_BASE}/tickets"
    skip = 0
    fil = "(baseStatus eq 'New' or baseStatus eq 'InAttendance' or baseStatus eq 'Stopped')"
    sel = ",".join(
        [
            "id",
            "subject",
            "type",
            "status",
            "baseStatus",
            "ownerTeam",
            "serviceFirstLevel",
            "serviceSecondLevel",
            "serviceThirdLevel",
            "origin",
            "category",
            "urgency",
            "createdDate",
            "lastUpdate",
        ]
    )
    exp = "clients($expand=organization),createdBy"
    items = []
    while True:
        page = req(
            url,
            {
                "token": TOKEN,
                "$select": sel,
                "$expand": exp,
                "$filter": fil,
                "$orderby": "lastUpdate asc",
                "$top": TOP,
                "$skip": skip,
            },
        ) or []
        if not page:
            break
        items.extend(page)
        if len(page) < TOP:
            break
        skip += len(page)
        time.sleep(THROTTLE)
    return items


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


def map_row(t):
    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    org = c0.get("organization") or {}
    created_by = t.get("createdBy") or {}
    created_val = norm_ts(t.get("createdDate"))
    return {
        "id": iint(t.get("id")) if str(t.get("id", "")).isdigit() else t.get("id"),
        "subject": t.get("subject"),
        "type": t.get("type"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "owner_team": t.get("ownerTeam"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "created_date": created_val,
        "last_update": norm_ts(t.get("lastUpdate")),
        "empresa_id": org.get("id"),
        "empresa_nome": org.get("businessName"),
        "requester_id": created_by.get("id"),
        "requester_name": created_by.get("businessName"),
    }


def get_table_columns(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'visualizacao'
              and table_name = 'movidesk_tickets_abertos'
            order by ordinal_position
            """
        )
        return [r[0] for r in cur.fetchall()]


def build_sql(cols):
    cols_wo_id = [c for c in cols if c != "id"]
    insert_cols = ",".join(cols)
    values = ",".join([f"%({c})s" for c in cols])
    sets = ",".join([f"{c}=excluded.{c}" for c in cols_wo_id])
    return f"""
    insert into visualizacao.movidesk_tickets_abertos ({insert_cols})
    values ({values})
    on conflict (id) do update set {sets}
    """


def upsert_rows(conn, rows):
    table_cols = get_table_columns(conn)
    wanted = [
        "id",
        "subject",
        "type",
        "status",
        "base_status",
        "owner_team",
        "service_first_level",
        "service_second_level",
        "service_third_level",
        "origin",
        "category",
        "urgency",
        "created_date",
        "last_update",
        "empresa_id",
        "empresa_nome",
        "requester_id",
        "requester_name",
    ]
    cols = [c for c in wanted if c in table_cols]
    if not cols:
        return 0
    sql = build_sql(cols)
    payload = [{c: r.get(c) for c in cols} for r in rows]
    with conn.cursor() as cur:
        cur.execute("truncate table visualizacao.movidesk_tickets_abertos")
        if payload:
            psycopg2.extras.execute_batch(cur, sql, payload, page_size=200)
    conn.commit()
    return len(rows)


def main():
    if not TOKEN or not DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    items = fetch_open()
    rows = [map_row(t) for t in items if isinstance(t, dict) and t.get("id") is not None]
    with psycopg2.connect(DSN) as conn:
        upsert_rows(conn, rows)


if __name__ == "__main__":
    main()
