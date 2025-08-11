import os, time, json, requests, psycopg2
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
BASE      = "https://api.movidesk.com/public/v1"

def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r
    r.raise_for_status()

def fetch_paginated(endpoint, params=None, page_size=200, hard_limit=20000):
    p = dict(params or {})
    p["token"] = API_TOKEN
    p["$top"]  = page_size
    items, skip = [], 0
    while True:
        p["$skip"] = skip
        r = get_with_retry(f"{BASE}/{endpoint}", p)
        batch = r.json() or []
        if not isinstance(batch, list): batch = [batch]
        items.extend(batch)
        if len(batch) < page_size or len(items) >= hard_limit:
            break
        skip += page_size
    return items

def fetch_agents():
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile"]
    params = {"$select": ",".join(select_fields), "$expand": "emails,teams", "$filter": "(profileType eq 1 or profileType eq 3)"}
    data = fetch_paginated("persons", params, page_size=200)
    if data:
        print(f"[people] filtro agentes: {len(data)}")
        return data
    print("[people] sem retorno no filtro; tentando sem filtro")
    params = {"$select": ",".join(select_fields), "$expand": "emails,teams"}
    data = fetch_paginated("persons", params, page_size=200)
    print(f"[people] sem filtro: {len(data)}")
    return data

def pick_email(person):
    emails = person.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"):
                return e.get("email") or ""
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType","")).lower() in ("professional","profissional","commercial","comercial"):
                return e.get("email") or ""
        first = emails[0]
        if isinstance(first, dict): return first.get("email") or ""
        if isinstance(first, str):  return first
    return ""

def pick_teams(person):
    teams = person.get("teams") or []
    out = []
    for t in teams:
        if isinstance(t, dict):
            out.append(t.get("businessName") or t.get("name") or t.get("title"))
        elif isinstance(t, str):
            out.append(t)
    out = [x for x in out if x]
    return out or None

def extract_access(person):
    return person.get("accessProfile") or person.get("businessProfile") or (person.get("profile") or {}).get("type", "") or ""

def extract_active(person):
    v = person.get("isActive", person.get("active"))
    if isinstance(v, bool): return v
    if isinstance(v, str):  return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

def normalize(people):
    rows, raw_count = [], 0
    for p in people:
        raw_count += 1
        pt = p.get("profileType")
        if pt not in (1, 3):
            continue
        pid = p.get("id")
        try:
            pid = int(pid)
        except Exception:
            continue
        teams = pick_teams(p)
        rows.append({
            "agent_id": pid,
            "name": p.get("businessName") or p.get("name") or "",
            "email": pick_email(p),
            "team_primary": teams[0] if teams else None,
            "teams": teams,
            "access_type": extract_access(p),
            "is_active": extract_active(p),
            "raw": p
        })
    print(f"[normalize] recebidos: {raw_count} | agentes: {len(rows)}")
    if rows:
        sample = {k: rows[0][k] for k in ("agent_id","name","email","team_primary","access_type","is_active")}
        print("[sample]", json.dumps(sample, ensure_ascii=False))
    return rows

DDL = """
create schema if not exists visualizacao_agentes;
create table if not exists visualizacao_agentes.agentes (
  agent_id     bigint primary key,
  name         text not null,
  email        text not null,
  team_primary text,
  teams        text[],
  access_type  text,
  is_active    boolean not null,
  raw          jsonb,
  updated_at   timestamptz not null default now()
);
create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));
create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);
"""

UPSERT = """
insert into visualizacao_agentes.agentes
  (agent_id, name, email, team_primary, teams, access_type, is_active, raw, updated_at)
values
  (%(agent_id)s, %(name)s, %(email)s, %(team_primary)s, %(teams)s, %(access_type)s, %(is_active)s, %(raw)s, now())
on conflict (agent_id) do update set
  name        = excluded.name,
  email       = excluded.email,
  team_primary= excluded.team_primary,
  teams       = excluded.teams,
  access_type = excluded.access_type,
  is_active   = excluded.is_active,
  raw         = excluded.raw,
  updated_at  = now();
"""

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def upsert(conn, rows):
    if not rows:
        print("[db] nenhum agente para gravar.")
        return
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(UPSERT, {**r, "raw": Json(r["raw"])})
    conn.commit()
    print(f"[db] upsert: {len(rows)}")

def main():
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("select current_database(), current_user"); print("Conectado em:", cur.fetchone())
    ensure_structure(conn)
    print("Consultando pessoas…")
    people = fetch_agents()
    rows = normalize(people)
    upsert(conn, rows)
    with conn.cursor() as cur:
        cur.execute("select count(*) from visualizacao_agentes.agentes")
        print("Linhas na tabela:", cur.fetchone()[0])
    conn.close()
    print("Fim.")

if __name__ == "__main__":
    main()
