import os, time, random, json, re, sys
from unidecode import unidecode
import pandas as pd
import requests
import psycopg2

API_TOKEN = os.getenv("MOVIDESK_TOKEN","").strip()
DSN = os.getenv("NEON_DSN","").strip()
CSV_ENV = os.getenv("FIELDS_CSV_PATH","").strip()

def log(x): print(f"[sync_companies] {x}", flush=True)

# ---------------- HTTP ----------------
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
            if filter_expr: params["$filter"] = filter_expr
            r = get_with_retry(s, url, params)
            batch = r.json()
            if not batch: break
            items.extend(batch)
            if len(batch) < top: break
            skip += len(batch)
            time.sleep(6.5)
    return items

def fetch_companies():
    items = fetch_companies_filtered("personType eq 2")
    if items: return items
    all_items = fetch_companies_filtered(None)
    return [x for x in all_items if str(x.get("personType")) == "2"]

# ---------------- helpers ----------------
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
        m = dict(zip(df["Id"].tolist(), df["Nome"].tolist()))
        log(f"CSV carregado: {len(m)} campos adicionais")
        return m
    except Exception as e:
        log(f"AVISO: falha ao ler CSV: {e}")
        return {}

def sanitize_column(name):
    b = unidecode(str(name)).lower()
    b = re.sub(r"[^a-z0-9_]+","_", b).strip("_")
    return (b if b else "campo")[:60]

def scalarize(v):
    if isinstance(v, (str, int, float, bool)) or v is None: return v
    return json.dumps(v, ensure_ascii=False)

def discover_standard_columns(items):
    base = {"id","businessName","corporateName","cpfCnpj","isActive","personType"}
    for it in items:
        for k, v in it.items():
            if k == "customFieldValues": continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                base.add(k)
    return sorted(base)

def discover_cf_ids(items):
    ids = set()
    for it in items:
        for cv in it.get("customFieldValues") or []:
            fid = cv.get("customFieldId")
            if fid is not None: ids.add(int(fid))
    return sorted(ids)

def flatten_cf_value(cv):
    v = cv.get("value")
    items = cv.get("items") or []
    if items:
        vals = []
        for it in items:
            for key in ("customFieldItem","team","personId","clientId"):
                if it.get(key) not in (None,""):
                    vals.append(str(it.get(key))); break
        if vals: v = "; ".join(vals)
    return v if v is None or isinstance(v, str) else json.dumps(v, ensure_ascii=False)

def build_cf_columns(cf_ids, field_map):
    cols, by_id, used = [], {}, set()
    for fid in cf_ids:
        name = field_map.get(fid, f"cf_{fid}")
        col = sanitize_column(name)
        if col in used: col = f"{col}_{fid}"
        used.add(col)
        cols.append(col)
        by_id[fid] = {"name": name, "col": col}
    return cols, by_id

# ---------------- NEON (estrutura) ----------------
def catalog_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select 1
              from information_schema.tables
             where table_schema='visualizacao_atual'
               and table_name='movidesk_companies_columns'
            limit 1;
        """)
        return cur.fetchone() is not None

def apply_catalog_and_function(conn, std_cols, by_id):
    with conn.cursor() as cur:
        # insere/atualiza catálogo
        for c in std_cols:
            col = sanitize_column(c)
            cur.execute("""
            insert into visualizacao_atual.movidesk_companies_columns (col_name, display_name, is_standard, source)
            values (%s,%s,true,'std')
            on conflict (col_name) do update set display_name=excluded.display_name, is_standard=excluded.is_standard, source=excluded.source;
            """, (col, c))
        for fid, meta in by_id.items():
            cur.execute("""
            insert into visualizacao_atual.movidesk_companies_columns (col_name, display_name, is_standard, source)
            values (%s,%s,false,'cf')
            on conflict (col_name) do update set display_name=excluded.display_name, is_standard=excluded.is_standard, source=excluded.source;
            """, (meta["col"], meta["name"]))
        # chama função que garante colunas na wide
        cur.execute("select visualizacao_atual.ensure_company_columns();")
    conn.commit()

def ensure_wide_direct(conn, std_cols, cf_cols):
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists visualizacao_atual.movidesk_companies_wide (
          id varchar(64) primary key
        );
        """)
        for c in std_cols:
            cur.execute(f'alter table visualizacao_atual.movidesk_companies_wide add column if not exists "{sanitize_column(c)}" text;')
        for c in cf_cols:
            cur.execute(f'alter table visualizacao_atual.movidesk_companies_wide add column if not exists "{c}" text;')
    conn.commit()

# ---------------- NEON (dados) ----------------
def upsert_raw(conn, items):
    with conn.cursor() as cur:
        for it in items:
            cur.execute("""
            insert into visualizacao_atual.movidesk_companies_raw (id, raw)
            values (%s, %s)
            on conflict (id) do update set raw=excluded.raw;
            """, (str(it.get("id")), json.dumps(it, ensure_ascii=False)))
    conn.commit()

def load_eav(conn, items, by_id):
    with conn.cursor() as cur:
        ids = [str(i.get("id")) for i in items if i.get("id") is not None]
        if ids:
            cur.execute("delete from visualizacao_atual.movidesk_company_custom_fields where company_id = any(%s);", (ids,))
        for it in items:
            for cv in (it.get("customFieldValues") or []):
                fid = cv.get("customFieldId")
                if fid is None: continue
                meta = by_id.get(int(fid))
                if not meta: continue
                val_txt = flatten_cf_value(cv)
                val_txt = "" if val_txt is None else str(val_txt)
                cur.execute("""
                insert into visualizacao_atual.movidesk_company_custom_fields
                  (company_id,custom_field_id,custom_field_name,rule_id,line,value_text,value_raw)
                values (%s,%s,%s,%s,%s,%s,%s)
                on conflict do nothing;
                """, (str(it.get("id")), int(fid), meta["name"], cv.get("customFieldRuleId"),
                      cv.get("line"), val_txt, json.dumps(cv, ensure_ascii=False)))
    conn.commit()

def upsert_wide(conn, items, std_cols, by_id, cf_cols):
    std_cols_sanit = [sanitize_column(c) for c in std_cols]
    cols = ["id"] + [c for c in std_cols_sanit if c != "id"] + cf_cols
    placeholders = ",".join(["%s"]*len(cols))
    upsert = f"""
    insert into visualizacao_atual.movidesk_companies_wide ({",".join(['"'+c+'"' for c in cols])})
    values ({placeholders})
    on conflict ("id") do update set {",".join(['"'+c+'"=excluded."'+c+'"' for c in cols if c!='id'])};
    """
    with conn.cursor() as cur:
        for it in items:
            row = {}
            for c in std_cols:
                row[sanitize_column(c)] = scalarize(it.get(c))
            for cv in (it.get("customFieldValues") or []):
                fid = cv.get("customFieldId")
                if fid is None: continue
                meta = by_id.get(int(fid))
                if not meta: continue
                row[meta["col"]] = flatten_cf_value(cv)
            vals = [str(it.get("id"))] + [None if row.get(c) is None else str(row.get(c)) for c in cols if c!="id"]
            cur.execute(upsert, vals)
    conn.commit()

# ---------------- main ----------------
def main():
    if not API_TOKEN or not DSN:
        log("ERRO: defina MOVIDESK_TOKEN e NEON_DSN."); sys.exit(2)

    field_map = try_load_person_fields(CSV_ENV)
    items = fetch_companies()
    std_cols = discover_standard_columns(items)
    cf_ids = discover_cf_ids(items)
    cf_cols, by_id = build_cf_columns(cf_ids, field_map)

    conn = psycopg2.connect(DSN)

    if catalog_exists(conn):
        log("Catálogo encontrado: usando ensure_company_columns()")
        apply_catalog_and_function(conn, std_cols, by_id)
    else:
        log("Catálogo NÃO encontrado: adicionando colunas direto na WIDE")
        ensure_wide_direct(conn, std_cols, cf_cols)

    upsert_raw(conn, items)
    load_eav(conn, items, by_id)
    upsert_wide(conn, items, std_cols, by_id, cf_cols)
    conn.close()

    log(f"empresas coletadas: {len(items)}, colunas padrão: {len(std_cols)}, adicionais: {len(cf_cols)}")

if __name__ == "__main__":
    main()
