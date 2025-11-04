import time, requests, pandas as pd, psycopg2

TOKEN = "SEU_TOKEN_MOVIDESK"
NEON_URL = "SUA_NEON_DATABASE_URL"
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

while True:
    url = f"{BASE}?token={TOKEN}&$filter=personType eq 2&$select=id,businessName,customFieldValues&$expand=customFieldValues&$orderby=id asc&$top={TOP}&$skip={skip}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
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
    time.sleep(0.5)

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
