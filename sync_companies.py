import os
import time
import json
import requests
import psycopg2
import psycopg2.extras
from psycopg2 import sql

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

DB_SCHEMA = os.getenv("DB_SCHEMA", "visualizacao_empresa")
TABLE_NAME = os.getenv("TABLE_NAME", "empresas")
PRUNE_COLUMNS = os.getenv("PRUNE_COLUMNS", "true").lower() in ("1","true","yes","y")

PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "300"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

CF_31679 = int(os.getenv("CF_31679", "31679"))
CF_217664 = int(os.getenv("CF_217664", "217664"))
CF_217659 = int(os.getenv("CF_217659", "217659"))
CF_217662 = int(os.getenv("CF_217662", "217662"))

KEEP_COLS = [
    "id",
    "businessname",
    "corporatename",
    "cpfcnpj",
    "isactive",
    "persontype",
    "username",
    "classification",
    "timezoneid",
    "createdby",
    "createddate",
    "changedby",
    "changeddate",
    "observations",
    "codereferenceadditional",
    "accessprofile",
    "tenant_url_do_hits",
    "property",
    "nome_na_omie",
    "cf_217662",
]

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
                print("HTTP ERROR", r.status_code, r.text[:1200])
            except Exception:
                pass
            r.raise_for_status()
        return r.json() if r.text else []

def to_text(v):
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        parts = []
        for it in v:
            if isinstance(it, dict):
                parts.append(to_text(it.get("customFieldItem") or it.get("text") or it.get("value") or it.get("name") or it.get("title") or it.get("label") or it))
            else:
                parts.append(to_text(it))
        parts = [p for p in parts if p]
        return " | ".join(parts) if parts else None
    if isinstance(v, dict):
        inner = v.get("customFieldItem") or v.get("text") or v.get("value") or v.get("name") or v.get("title") or v.get("label")
        if inner is not None:
            return to_text(inner)
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)

def _get_cf_value(cf_list, field_id):
    if not isinstance(cf_list, list):
        return None
    for e in cf_list:
        try:
            if int(str(e.get("customFieldId"))) != int(field_id):
                continue
        except Exception:
            continue
        v = e.get("value") or e.get("text") or e.get("optionValue") or e.get("items") or e.get("item")
        t = to_text(v)
        if t:
            return t
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

def g(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d and d.get(k) is not None:
            return d.get(k)
    return None

def normalize(p):
    cf = p.get("customFieldValues") or []
    return {
        "id": to_text(g(p, "id")),
        "businessname": to_text(g(p, "businessName")),
        "corporatename": to_text(g(p, "corporateName")),
        "cpfcnpj": to_text(g(p, "cpfCnpj")),
        "isactive": to_text(g(p, "isActive")),
        "persontype": to_text(g(p, "personType")),
        "username": to_text(g(p, "username")),
        "classification": to_text(g(p, "classification")),
        "timezoneid": to_text(g(p, "timeZoneId")),
        "createdby": to_text(g(p, "createdBy")),
        "createddate": to_text(g(p, "createdDate", "createDate")),
        "changedby": to_text(g(p, "changedBy")),
        "changeddate": to_text(g(p, "changedDate")),
        "observations": to_text(g(p, "observation", "observations")),
        "codereferenceadditional": to_text(g(p, "codeReferenceAdditional")),
        "accessprofile": to_text(g(p, "accessProfile")),
        "tenant_url_do_hits": _get_cf_value(cf, CF_31679),
        "property": _get_cf_value(cf, CF_217664),
        "nome_na_omie": _get_cf_value(cf, CF_217659),
        "cf_217662": _get_cf_value(cf, CF_217662),
    }

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(sql.SQL('create schema if not exists {}').format(sql.Identifier(DB_SCHEMA)))
        cur.execute(sql.SQL('set search_path to {}').format(sql.Identifier(DB_SCHEMA)))
    conn.commit()

def ensure_table(conn):
    cols_def = ", ".join([f"{c} text" for c in KEEP_COLS if c != "id"])
    with conn.cursor() as cur:
        cur.execute(sql.SQL('create table if not exists {}.{} (id text primary key)').format(sql.Identifier(DB_SCHEMA), sql.Identifier(TABLE_NAME)))
        for c in KEEP_COLS:
            if c == "id":
                continue
            cur.execute(sql.SQL("alter table {}.{} add column if not exists {} text").format(sql.Identifier(DB_SCHEMA), sql.Identifier(TABLE_NAME), sql.Identifier(c)))
    conn.commit()

def prune_unused_columns(conn):
    if not PRUNE_COLUMNS:
        return
    with conn.cursor() as cur:
        cur.execute("""
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
        """, (DB_SCHEMA, TABLE_NAME))
        existing = [r[0] for r in cur.fetchall()]
    to_drop = [c for c in existing if c not in KEEP_COLS]
    if not to_drop:
        return
    with conn.cursor() as cur:
        for c in to_drop:
            q = sql.SQL("alter table {}.{} drop column if exists {} cascade").format(
                sql.Identifier(DB_SCHEMA), sql.Identifier(TABLE_NAME), sql.Identifier(c)
            )
            cur.execute(q)
    conn.commit()

def upsert_rows(conn, rows):
    rows = [r for r in rows if r.get("id")]
    if not rows:
        return 0
    cols = KEEP_COLS
    values = [[r.get(c) for c in cols] for r in rows]
    with conn.cursor() as cur:
        template = "(" + ",".join(["%s"] * len(cols)) + ")"
        insert_sql = sql.SQL("""
            insert into {}.{} ({cols}) values %s
            on conflict (id) do update set
            {updates}
        """).format(
            sql.Identifier(DB_SCHEMA),
            sql.Identifier(TABLE_NAME),
            cols=sql.SQL(",").join(map(sql.Identifier, cols)),
            updates=sql.SQL(",").join(
                sql.SQL("{}=excluded.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in cols if c != "id"
            ),
        )
        psycopg2.extras.execute_values(cur, insert_sql.as_string(conn), values, template=template, page_size=1000)
    conn.commit()
    return len(values)

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    data = fetch_companies()
    rows = [normalize(p) for p in data if isinstance(p, dict)]
    conn = psycopg2.connect(NEON_DSN)
    try:
        ensure_schema(conn)
        ensure_table(conn)
        prune_unused_columns(conn)
        n = upsert_rows(conn, rows)
        print(f"EMPRESAS upsert: {n}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
