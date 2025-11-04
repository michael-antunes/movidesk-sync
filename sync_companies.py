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

CF_TENANT_URL_HITS = int(os.getenv("CF_TENANT_URL_HITS", "31679"))
CF_PROPERTY = int(os.getenv("CF_PROPERTY", "217664"))
CF_NOME_OMIE = int(os.getenv("CF_NOME_OMIE", "217659"))

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
    select_fields = ",".join([
        "id","businessName","corporateName","cpfCnpj",
        "codeReferenceAdditional","isActive","createdDate","changedDate"
    ])
    expand = "customFieldValues"
    filtro = "personType eq 2"
    skip = 0
    items = []
    while True:
        page = _req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
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

def normalize(p):
    pid = iint(p.get("id"))
    cf = p.get("customFieldValues") or []
    tenant = _get_cf_value(cf, CF_TENANT_URL_HITS)
    prop = _get_cf_value(cf, CF_PROPERTY)
    omie = _get_cf_value(cf, CF_NOME_OMIE)
    return {
        "id": pid,
        "business_name": p.get("businessName"),
        "tenant_url_hits": tenant,
        "property": prop,
        "nome_na_omie": omie
    }

def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("alter table if exists empresa add column if not exists tenant_url_hits text")
        cur.execute("alter table if exists empresa add column if not exists property text")
        cur.execute("alter table if exists empresa add column if not exists nome_na_omie text")
    conn.commit()

def update_only(conn, rows):
    if not rows:
        return 0
    values = [(r["id"], r["tenant_url_hits"], r["property"], r["nome_na_omie"]) for r in rows if r.get("id") is not None]
    if not values:
        return 0
    with conn.cursor() as cur:
        template = "(%s,%s,%s,%s)"
        sql = """
            update empresa e
               set tenant_url_hits = v.tenant_url_hits,
                   property = v.property,
                   nome_na_omie = v.nome_na_omie
              from (values %s) as v(id, tenant_url_hits, property, nome_na_omie)
             where e.id = v.id
        """
        psycopg2.extras.execute_values(cur, sql, values, template=template)
    conn.commit()
    return len(values)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    companies = fetch_companies()
    rows = [normalize(p) for p in companies if isinstance(p, dict)]
    conn = psycopg2.connect(NEON_DSN)
    try:
        ensure_columns(conn)
        n = update_only(conn, rows)
        print(f"EMPRESAS atualizadas: {n}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
