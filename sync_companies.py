import os
import time
import requests
import psycopg2
import psycopg2.extras

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "300"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

CF_31679 = int(os.getenv("CF_31679", "31679"))
CF_217664 = int(os.getenv("CF_217664", "217664"))
CF_217659 = int(os.getenv("CF_217659", "217659"))
CF_217662 = int(os.getenv("CF_217662", "217662"))

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
        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:1000])
            except Exception:
                pass
            r.raise_for_status()
        return r.json() if r.text else []

def _first_item_label(e):
    items = e.get("items")
    if isinstance(items, list) and items:
        for it in items:
            if isinstance(it, dict):
                v = it.get("customFieldItem") or it.get("text") or it.get("value")
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None

def _get_cf_value(cf_list, field_id):
    if not isinstance(cf_list, list):
        return None
    for e in cf_list:
        try:
            if int(str(e.get("customFieldId"))) != int(field_id):
                continue
        except Exception:
            continue
        v = e.get("value") or e.get("text") or e.get("optionValue") or _first_item_label(e)
        if isinstance(v, str):
            return v.strip() or None
        return None
    return None

def fetch_companies():
    url = f"{API_BASE}/persons"
    filtro = "personType eq 2"
    expand = "customFieldValues"
    skip = 0
    items = []
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$expand": expand,
            "$filter": filtro,
            "$top": PAGE_SIZE,
            "$skip": skip
        }) or []
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < PAGE_SIZE:
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

def bbool(x):
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        t = x.strip().lower()
        if t in ("true","1","t","y","yes","sim"):
            return True
        if t in ("false","0","f","n","no","nao","não"):
            return False
    return None

def g(obj, *keys):
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            v = obj.get(k)
            if v is not None:
                return v
    return None

def normalize(p):
    cf = p.get("customFieldValues") or []
    return {
        "id": iint(p.get("id")),
        "bussineessname": g(p, "businessName"),
        "corporatename": g(p, "corporateName"),
        "cpfcnpj": g(p, "cpfCnpj"),
        "isactive": bbool(g(p, "isActive")),
        "persontype": iint(g(p, "personType")),
        "username": g(p, "username"),
        "classification": g(p, "classification"),
        "timezoneid": g(p, "timeZoneId"),
        "createdby": g(p, "createdBy"),
        "createdate": g(p, "createdDate"),
        "chagebdy": g(p, "changedBy"),
        "changeddate": g(p, "changedDate"),
        "observation": g(p, "observation"),
        "codereferenceadditionalaccessprofile": g(p, "codeReferenceAdditionalAccessProfile"),
        "cf_217662": _get_cf_value(cf, CF_217662),
        "cf_31679": _get_cf_value(cf, CF_31679),
        "cf_217664": _get_cf_value(cf, CF_217664),
        "cf_217659": _get_cf_value(cf, CF_217659),
    }

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists empresa(
            id bigint primary key,
            bussineessname text,
            corporatename text,
            cpfcnpj text,
            isactive boolean,
            persontype int,
            username text,
            classification text,
            timezoneid text,
            createdby text,
            createdate timestamptz,
            chagebdy text,
            changeddate timestamptz,
            observation text,
            codereferenceadditionalaccessprofile text,
            cf_217662 text,
            cf_31679 text,
            cf_217664 text,
            cf_217659 text
        )
        """)
        cur.execute("alter table empresa add column if not exists bussineessname text")
        cur.execute("alter table empresa add column if not exists corporatename text")
        cur.execute("alter table empresa add column if not exists cpfcnpj text")
        cur.execute("alter table empresa add column if not exists isactive boolean")
        cur.execute("alter table empresa add column if not exists persontype int")
        cur.execute("alter table empresa add column if not exists username text")
        cur.execute("alter table empresa add column if not exists classification text")
        cur.execute("alter table empresa add column if not exists timezoneid text")
        cur.execute("alter table empresa add column if not exists createdby text")
        cur.execute("alter table empresa add column if not exists createdate timestamptz")
        cur.execute("alter table empresa add column if not exists chagebdy text")
        cur.execute("alter table empresa add column if not exists changeddate timestamptz")
        cur.execute("alter table empresa add column if not exists observation text")
        cur.execute("alter table empresa add column if not exists codereferenceadditionalaccessprofile text")
        cur.execute("alter table empresa add column if not exists cf_217662 text")
        cur.execute("alter table empresa add column if not exists cf_31679 text")
        cur.execute("alter table empresa add column if not exists cf_217664 text")
        cur.execute("alter table empresa add column if not exists cf_217659 text")
    conn.commit()

def upsert_rows(conn, rows):
    rows = [r for r in rows if r.get("id") is not None]
    if not rows:
        return 0
    cols = [
        "id",
        "bussineessname",
        "corporatename",
        "cpfcnpj",
        "isactive",
        "persontype",
        "username",
        "classification",
        "timezoneid",
        "createdby",
        "createdate",
        "chagebdy",
        "changeddate",
        "observation",
        "codereferenceadditionalaccessprofile",
        "cf_217662",
        "cf_31679",
        "cf_217664",
        "cf_217659"
    ]
    values = [[r.get(c) for c in cols] for r in rows]
    with conn.cursor() as cur:
        template = "(" + ",".join(["%s"] * len(cols)) + ")"
        insert_sql = f"""
            insert into empresa ({",".join(cols)}) values %s
            on conflict (id) do update set
                bussineessname=excluded.bussineessname,
                corporatename=excluded.corporatename,
                cpfcnpj=excluded.cpfcnpj,
                isactive=excluded.isactive,
                persontype=excluded.persontype,
                username=excluded.username,
                classification=excluded.classification,
                timezoneid=excluded.timezoneid,
                createdby=excluded.createdby,
                createdate=excluded.createdate,
                chagebdy=excluded.chagebdy,
                changeddate=excluded.changeddate,
                observation=excluded.observation,
                codereferenceadditionalaccessprofile=excluded.codereferenceadditionalaccessprofile,
                cf_217662=excluded.cf_217662,
                cf_31679=excluded.cf_31679,
                cf_217664=excluded.cf_217664,
                cf_217659=excluded.cf_217659
        """
        psycopg2.extras.execute_values(cur, insert_sql, values, template=template, page_size=1000)
    conn.commit()
    return len(values)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    data = fetch_companies()
    rows = [normalize(p) for p in data if isinstance(p, dict)]
    conn = psycopg2.connect(NEON_DSN)
    try:
        ensure_table(conn)
        n = upsert_rows(conn, rows)
        print(f"EMPRESAS upsert: {n}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
