import os, time, random, json, re
from unidecode import unidecode
import pandas as pd
import requests
import psycopg2

API_TOKEN = os.getenv("MOVIDESK_TOKEN","").strip()
DSN = os.getenv("NEON_DSN","").strip()
FIELDS_CSV_PATH = "data/Campos adicionais do Movidesk.csv"

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

def load_person_fields(csv_path):
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
    df = df.rename(columns={c: c.strip() for c in df.columns})
    df = df[df["Campo para"].str.strip().str.lower().eq("pessoa")]
    df = df[df.get("Ativo","Sim").astype(str).str.strip().str.lower().isin(["sim","true","1","ativo"])]
    df = df[["Id","Nome"]].dropna()
    df["Id"] = df["Id"].astype(int)
    return dict(zip(df["Id"].tolist(), df["Nome"].tolist()))

def sanitize_column(name):
    b = unidecode(str(name)).lower()
    b = re.sub(r"[^a-z0-9_]+","_", b).strip("_")
    return (b if b else "campo")[:60]

def fetch_companies():
    url = "https://api.movidesk.com/public/v1/persons"
    items, skip, top = [], 0, 100
    with requests.Session() as s:
        while True:
            params = {
                "token": API_TOKEN,
                "$filter": "personType eq 2",
                "$expand": "customFieldValues",
                "$top": top,
                "$skip": skip
            }
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

def scalarize(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return json.dumps(v, ensure_ascii=False)

def discover_standard_columns(items):
    keys = set(["id"])
    for it in items:
        for k, v in it.items():
            if k == "customFieldValues":
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                keys.add(k)
    return sorted(keys)

def ensure_schema(conn, std_cols, cf_names):
    with conn.cursor() as cur:
        cur.execute("""
        create schema if not exists visualizacao_atual;
        create table if not exists visualizacao_atual.movidesk_companies_raw (
          id varchar(64) primary key,
          raw jsonb not null
        );
        create table if not exists visualizacao_atual.movidesk_company_custom_fields (
          company_id varchar(64) not null,
          custom_field_id int not null,
          custom_field_name varchar(256) not null,
          rule_id int,
          line int,
          value_text text not null default '',
          value_raw jsonb,
          primary key (company_id, custom_field_id, coalesce(rule_id,0), coalesce(line,0), value_text)
        );
        """)
        std_cols_sql = ", ".join([f"\"{sanitize_column(c)}\" text" for c in std_cols])
        cf_cols_sql = ", ".join([f"\"{sanitize_column(c)}\" text" for c in cf_names])
        cur.execute("drop table if exists visualizacao_atual.movidesk_companies_wide;")
        cur.execute(f"""
        create table visualizacao_atual.movidesk_companies_wide (
          {std_cols_sql}{"," if cf_cols_sql else ""}{cf_cols_sql},
          primary key ("id")
        );
        """)
    conn.commit()

def upsert_raw(conn, items):
    with conn.cursor() as cur:
        cur.execute("truncate table visualizacao_atual.movidesk_companies_raw;")
        for it in items:
            cur.execute("""
            insert into visualizacao_atual.movidesk_companies_raw (id, raw)
            values (%s, %s)
            on conflict (id) do update set raw=excluded.raw;
            """, (str(it.get("id")), json.dumps(it, ensure_ascii=False)))
    conn.commit()

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

def load_eav_and_wide(conn, items, field_map, std_cols):
    with conn.cursor() as cur:
        cur.execute("truncate table visualizacao_atual.movidesk_company_custom_fields;")
        cur.execute("truncate table visualizacao_atual.movidesk_companies_wide;")
        all_cf_names = [field_map[k] for k in sorted(field_map.keys())]
        cf_cols = [sanitize_column(n) for n in all_cf_names]
        std_cols_sanit = [sanitize_column(c) for c in std_cols]
        cols = std_cols_sanit + cf_cols
        placeholders = ",".join(["%s"]*len(cols))
        insert_wide = f"""
        insert into visualizacao_atual.movidesk_companies_wide ({",".join(['"'+c+'"' for c in cols])})
        values ({placeholders})
        on conflict ("id") do update set {",".join(['"'+c+'"=excluded."'+c+'"' for c in cols if c!='id'])};
        """
        insert_eav = """
        insert into visualizacao_atual.movidesk_company_custom_fields
          (company_id,custom_field_id,custom_field_name,rule_id,line,value_text,value_raw)
        values (%s,%s,%s,%s,%s,%s,%s)
        on conflict do nothing;
        """
        for it in items:
            row_std = []
            for c in std_cols:
                v = scalarize(it.get(c))
                row_std.append(None if v is None else str(v))
            cf_values = {sanitize_column(field_map[fid]): None for fid in field_map}
            for cv in (it.get("customFieldValues") or []):
                fid = cv.get("customFieldId")
                if fid not in field_map:
                    continue
                name = field_map[fid]
                col = sanitize_column(name)
                val_txt = flatten_cf_value(cv)
                val_txt = "" if val_txt is None else str(val_txt)
                cf_values[col] = val_txt
                cur.execute(insert_eav, (
                    str(it.get("id")), int(fid), name, cv.get("customFieldRuleId"),
                    cv.get("line"), val_txt, json.dumps(cv, ensure_ascii=False)
                ))
            row = row_std + [cf_values[c] for c in cf_cols]
            cur.execute(insert_wide, row)
    conn.commit()

def main():
    field_map = load_person_fields(FIELDS_CSV_PATH)
    items = fetch_companies()
    std_cols = discover_standard_columns(items)
    cf_names = [field_map[k] for k in sorted(field_map.keys())]
    conn = psycopg2.connect(DSN)
    ensure_schema(conn, std_cols, cf_names)
    upsert_raw(conn, items)
    load_eav_and_wide(conn, items, field_map, std_cols)
    conn.close()

if __name__ == "__main__":
    main()
