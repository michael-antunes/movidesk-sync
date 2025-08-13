import os, time, requests, psycopg2
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN", "")
DSN = os.getenv("NEON_DSN", "")
BASE = "https://api.movidesk.com/public/v1"

def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r
    r.raise_for_status()

def fetch_paginated(endpoint, params=None, page_size=200):
    p = dict(params or {})
    p["token"] = API_TOKEN
    p["$top"] = page_size
    items, skip = [], 0
    while True:
        p["$skip"] = skip
        r = get_with_retry(f"{BASE}/{endpoint}", p)
        batch = r.json() or []
        if not isinstance(batch, list): batch = [batch]
        items.extend(batch)
        if len(batch) < page_size: break
        skip += page_size
    return items

def fetch_agents():
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile","teams"]
    params = {"$select": ",".join(select_fields), "$expand": "emails($select=email,isDefault,emailType)"}
    return fetch_paginated("persons", params, page_size=200)

def pick_email(p):
    emails = p.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"):
                v = (e.get("email") or "").strip()
                if v: return v
        pref = ("professional","profissional","commercial","comercial")
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType","")).lower() in pref:
                v = (e.get("email") or "").strip()
                if v: return v
        f = emails[0]
        if isinstance(f, dict): return (f.get("email") or "").strip()
        if isinstance(f, str): return f.strip()
    return (p.get("userName") or "").strip()

def extract_access(p):
    return str(p.get("accessProfile") or p.get("businessProfile") or (p.get("profile") or {}).get("type","") or "")

def extract_active(p):
    v = p.get("isActive", p.get("active"))
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

def pick_teams(p):
    t = p.get("teams")
    if isinstance(t, list):
        out = []
        for x in t:
            if isinstance(x, dict):
                n = (x.get("businessName") or x.get("name") or "").strip()
                if n: out.append(n)
            elif isinstance(x, str):
                s = x.strip()
                if s: out.append(s)
        return out or None
    return None

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
create or replace view visualizacao_agentes.vw_agentes_resumida as
select agent_id,name,email,coalesce(team_primary, teams[1]) as equipe,access_type,is_active
from visualizacao_agentes.agentes;
"""

UPSERT = """
insert into visualizacao_agentes.agentes
  (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at)
values
  (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now())
on conflict (agent_id) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  is_active=excluded.is_active,
  raw=excluded.raw,
  updated_at=now();
"""

def ensure_structure(conn):
    with conn.cursor() as cur: cur.execute(DDL)
    conn.commit()

def upsert(conn, rows):
    if not rows: return
    with conn.cursor() as cur:
        for r in rows: cur.execute(UPSERT, {**r, "raw": Json(r["raw"])})
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    people = [p for p in fetch_agents() if p.get("profileType") in (1,3)]
    rows = []
    for p in people:
        try: pid = int(p.get("id"))
        except: continue
        teams = pick_teams(p)
        rows.append({
            "agent_id": pid,
            "name": (p.get("businessName") or p.get("name") or "").strip(),
            "email": pick_email(p),
            "team_primary": (teams or [None])[0],
            "teams": teams,
            "access_type": extract_access(p),
            "is_active": extract_active(p),
            "raw": p
        })
    upsert(conn, rows)
    conn.close()

if __name__ == "__main__":
    main()
