import os, time, requests, pandas as pd, psycopg2
from pathlib import Path

TOKEN_CONST = "COLOQUE_SEU_TOKEN_MOVIDESK_AQUI"
NEON_CONST = "COLOQUE_SUA_NEON_DATABASE_URL_AQUI"

def read_first_line(p):
    try:
        return Path(p).read_text(encoding="utf-8").strip()
    except:
        return None

TOKEN = os.getenv("MOVIDESK_TOKEN") or read_first_line("movidesk.token") or TOKEN_CONST
NEON_URL = os.getenv("NEON_DATABASE_URL") or read_first_line("neon.url") or NEON_CONST

if not TOKEN or TOKEN.startswith("COLOQUE_"):
    raise SystemExit("Faltou o token do Movidesk. Defina MOVIDESK_TOKEN, ou crie o arquivo movidesk.token, ou edite TOKEN_CONST.")
if not NEON_URL or NEON_URL.startswith("COLOQUE_"):
    raise SystemExit("Faltou a URL do Neon. Defina NEON_DATABASE_URL, ou crie o arquivo neon.url, ou edite NEON_CONST.")

BASE = "https://api.movidesk.com/public/v1/persons"
TOP = 100
skip = 0
rows = []

def pick_cf(cf_list, cf_id):
    for cf in cf_list or []:
        if str(cf.get("customFieldId")) == str(cf_id):
            v = cf.get("value")
            if v not in [None, ""]:
                return str(v)
            items = cf.get("items") or []
            if items:
                names = [i.get("customFieldItem") for i in items if i.get("customFieldItem")]
                if names:
                    return ", ".join(names)
    return None

def get_page(url):
    for i in range(5):
        r = requests.get(url, timeout=60)
        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(1.5 * (i + 1))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

while True:
    url = f"{BASE}?token={TOKEN}&$filter=personType eq 2&$select=id,businessName,customFieldValues&$expand=customFieldValues&$orderby=id asc&$top={TOP}&$skip={skip}"
    data = get_page(url)
    if not data:
        break
    for p in data:
        cfs = p.get("customFieldValues") or []
        rows.append({
            "id": str(p.get("id")),
            "businessname": p.get("businessName"),
            "nome_na_omie": pick_cf(cfs, 217659),
            "property": pick_cf(cfs, 217664),
            "cf_31679": pick_cf(cfs, 31679)
        })
    skip += TOP
    time.sleep(0.4)

df = pd.DataFrame(rows)
df.to_csv("empresas_movidesk.csv", index=False, encoding="utf-8")

conn = psycopg2.connect(NEON_URL)
cur = conn.cursor()
cur.execute("""
create table if not exists empresa (
  id text primary key,
  businessname text,
  nome_na_omie text,
  property text,
  cf_31679 text
)
""")
if not df.empty:
    args = [tuple(r) for r in df[["id","businessname","nome_na_omie","property","cf_31679"]].values]
    cur.executemany("""
    insert into empresa (id,businessname,nome_na_omie,property,cf_31679)
    values (%s,%s,%s,%s,%s)
    on conflict (id) do update set
      businessname=excluded.businessname,
      nome_na_omie=excluded.nome_na_omie,
      property=excluded.property,
      cf_31679=excluded.cf_31679
    """, args)
conn.commit()
cur.close()
conn.close()
print("OK")
