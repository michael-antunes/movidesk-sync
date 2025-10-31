import os, time, random, json, re, sys
from unidecode import unidecode
import pandas as pd
import requests
import psycopg2

API_TOKEN = os.getenv("MOVIDESK_TOKEN","").strip()
DSN = os.getenv("NEON_DSN","").strip()
CSV_ENV = os.getenv("FIELDS_CSV_PATH","").strip()

def get_with_retry(session, url, params):
    a = 0
    while True:
        r = session.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            a += 1
            time.sleep(min(60,(2**a)+random.random()))
            continue
        r.raise_for_status()
        return r

def fetch_companies_filtered(filter_expr=None):
    url = "https://api.movidesk.com/public/v1/persons"
    items, skip, top = [], 0, 100
    with requests.Session() as s:
        while True:
            params = {"token": API_TOKEN, "$expand": "customFieldValues", "$top": top, "$skip": skip}
            if filter_expr:
                params["$filter"] = filter_expr
            r = get_with_retry(s, url, params)
            batch = r.json()
            if not batch:
                break
            items.extend(batch)
            if len(batch) < top:
                break
            skip += len(batch)
            time.sleep(6.5)
    return items

def fetch_companies():
    try:
        x = fetch_companies_filtered("personType eq 2")
    except requests.HTTPError:
        x = None
    if x:
        return x
    try:
        y = fetch_companies_filtered(None)
    except requests.HTTPError:
        y = []
    return [i for i in y if str(i.get("personType")) == "2"]

def try_load_person_fields(csv_path):
    try:
        if not csv_path or not os.path.isfile(csv_path):
            return {}
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
        df = df.rename(columns={c: c.strip() for c in df.columns})
        df = df[df["Campo para"].str.strip().str.lower().eq("pessoa")]
        df = df[df.get("Ativo","Sim").astype(str).str.strip().str.lower().isin(["sim","true","1","ativo"])]
        df = df[["Id","Nome"]].dropna()
        df["Id"] = df["Id"].astype(int)
        return dict(zip(df["Id"].tolist(), df["Nome"].tolist()))
    except:
        return {}

def sanitize(name):
    b = unidecode(str(name)).lower()
    b = re.sub(r"[^a-z0-9_]+","_", b).strip("_")
    return (b if b else "campo")[:60]

def scalar(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return json.dumps(v, ensure_ascii=False)

BASE_STD = [
  "id","businessName","corporateName","cpfCnpj","isActive","personType","userName","accountEmail",
  "bossId","bossName","businessBranch","classification","cultureId","timeZoneId","createdBy",
  "createdDate","changedBy","changedDate","profileType","role","observations","authentication",
  "accessProfile","authenticateOn","codeReferenceAdditional"
]

def discover_std_cols(items):
    cols = set(BASE_STD)
    for it in items:
        for k, v in it.items():
            if k == "customFieldValues":
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                cols.add(k)
    return sorted(cols)

def discover_cf_ids(items):
    ids = set()
    for it in items:
        for cv in it.get("customFieldValues") or []:
            fid = cv.get("customFieldId")
            if fid is not None:
                ids.add(int(fid))
    return sorted(ids)

def flatten_cf_value(cv):
    v = cv.get("value")
    items = cv.get("items") or []
    if items:
        vals = []
        for it in items:
            for key in ("customFieldItem","team","personId","clientId"):
                if it.get(key) not in (None,""):
                    vals.append(str(it.get(key)))
                    break
        if vals:
            v = "; ".join(vals)
    return v if v is None or isinstance(v, str) else json.dumps(v, ensure_ascii=False)

def build_cf_cols(cf_ids, field_map):
    cols, by_id, used = [], {}, set()
    for fid in cf_ids:
        name = field_map.get(fid, f"cf_{fid}")
        col = sanitize(name)
        if col in used:
            col = f"{col}_{fid}"
        used.add(col)
        cols.append(col)
        by_id[fid] = {"name": name, "col": col}
    return cols, by_id

def phones_txt_from_item(it):
    phs = it.get("phones") or []
    out = []
    for p in phs:
        num = p.get("number") or p.get("phone") or p.get("value")
        if not num:
            continue
        num = str(num).strip()
        typ = p.get("phoneType") or p.get("type")
        out.append(f"{num} ({typ})" if typ else num)
    return "; ".join(out) if out else None

def ensure_table(conn, std_cols, cf_cols):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_empresa;")
        cur.execute("create table if not exists visualizacao_empresa.empresas (id varchar(64) primary key);")
        for c in std_cols:
            cur.execute(f'alter table visualizacao_empresa.empresas add column if not exists "{sanitize(c)}" text;')
        for c in cf_cols:
            cur.execute(f'alter table visualizacao_empresa.empresas add column if not exists "{c}" text;')
        cur.execute('alter table visualizacao_empresa.empresas add column if not exists "phones_txt" text;')
    conn.commit()

def replace_rows(conn, items, std_cols, by_id, cf_cols):
    std_cols_s = [sanitize(c) for c in std_cols]
    cols = ["id"] + [c for c in std_cols_s if c != "id"] + cf_cols + ["phones_txt"]
    placeholders = ",".join(["%s"]*len(cols))
    cols_sql = ",".join([f'"{c}"' for c in cols])
    ins = f'insert into visualizacao_empresa.empresas ({cols_sql}) values ({placeholders})'
    with conn.cursor() as cur:
        cur.execute("truncate table visualizacao_empresa.empresas;")
        for it in items:
            row = {}
            for c in std_cols:
                row[sanitize(c)] = scalar(it.get(c))
            for cv in (it.get("customFieldValues") or []):
                fid = cv.get("customFieldId")
                if fid is None:
                    continue
                meta = by_id.get(int(fid))
                if not meta:
                    continue
                row[meta["col"]] = flatten_cf_value(cv)
            row["phones_txt"] = phones_txt_from_item(it)
            vals = [str(it.get("id"))] + [None if row.get(c) is None else str(row.get(c)) for c in cols if c!="id"]
            cur.execute(ins, vals)
    conn.commit()

def main():
    if not API_TOKEN or not DSN:
        sys.exit(2)
    field_map = try_load_person_fields(CSV_ENV)
    items = fetch_companies()
    std_cols = discover_std_cols(items)
    cf_ids = discover_cf_ids(items)
    cf_cols, by_id = build_cf_cols(cf_ids, field_map)
    conn = psycopg2.connect(DSN)
    ensure_table(conn, std_cols, cf_cols)
    replace_rows(conn, items, std_cols, by_id, cf_cols)
    conn.close()

if __name__ == "__main__":
    main()
