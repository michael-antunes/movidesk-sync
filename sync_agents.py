import os, time, json, requests, psycopg2
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
BASE = "https://api.movidesk.com/public/v1"

def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r
    r.raise_for_status()

def fetch_paginated(endpoint, params=None, page_size=200, hard_limit=20000):
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
        if len(batch) < page_size or len(items) >= hard_limit: break
        skip += page_size
    return items

def fetch_agents():
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile"]
    try:
        params = {"$select": ",".join(select_fields), "$expand": "emails($select=email,isDefault,emailType),teams($select=businessName)"}
        data = fetch_paginated("persons", params, page_size=200)
        print(f"[people] expand ok: {len(data)}")
        return data
    except requests.HTTPError as e:
        print(f"[people] expand falhou: {e.response.status_code if e.response else '??'}; tentando sem expand")
        params = {"$select": ",".join(select_fields)}
        data = fetch_paginated("persons", params, page_size=200)
        print(f"[people] sem expand: {len(data)}")
        return data

def fetch_teams_index():
    try:
        teams = fetch_paginated("teams", {}, page_size=200)
    except requests.HTTPError as e:
        print(f"[teams] erro {e.response.status_code if e.response else '??'}"); return {}
    idx = {}
    for t in teams:
        name = t.get("businessName") or t.get("name") or t.get("title")
        members = t.get("agents") or t.get("members") or []
        for m in members:
            if isinstance(m, dict):
                aid = m.get("id") or m.get("agentId")
            else:
                aid = m
            if aid is None: continue
            try: aid = int(aid)
            except: continue
            idx.setdefault(aid, []).append(name)
    print(f"[teams] index para {len(idx)} agentes")
    return idx

def pick_email(p):
    emails = p.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"): return e.get("email") or ""
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType","")).lower() in ("professional","profissional","commercial","comercial"):
                return e.get("email") or ""
        first = emails[0]
        if isinstance(first, dict): return first.get("email") or ""
        if isinstance(first, str): return first
    return p.get("userName") or p.get("email") or p.get("businessEmail") or ""

def pick_teams(p):
    teams = p.get("teams") or []
    out = []
    for t in teams:
        if isinstance(t, dict): out.append(t.get("businessName") or t.get("name") or t.get("title"))
        elif isinstance(t, str): out.append(t)
    out = [x for x in out if x]
    return out or None

def extract_access(p):
    return p.get("accessProfile") or p.get("businessProfile") or (p.get("profile") or {}).get("type","") or ""

def extract_active(p):
    v = p.get("isActive", p.get("active"))
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

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
    if not rows: 
        print("[db] nenhum agente para gravar."); return
    with conn.cursor() as cur:
        for r in rows: cur.execute(UPSERT, {**r, "raw": Json(r["raw"])})
    conn.commit()
    print(f"[db] upsert: {len(rows)}")

def main():
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    people = fetch_agents()
    team_idx = fetch_teams_index()
    rows, total = [], 0
    for p in people:
        total += 1
        pt = p.get("profileType")
        if pt not in (1,3): continue
        pid = p.get("id")
        try: pid = int(pid)
        except: continue
        tnames = pick_teams(p)
        if not tnames and team_idx.get(pid): tnames = team_idx.get(pid)
        email = pick_email(p)
        row = {
            "agent_id": pid,
            "name": p.get("businessName") or p.get("name") or "",
            "email": email,
            "team_primary": (tnames or [None])[0],
            "teams": tnames,
            "access_type": extract_access(p),
            "is_active": extract_active(p),
            "raw": p
        }
        rows.append(row)
    print(f"[normalize] recebidos: {total} | agentes: {len(rows)}")
    upsert(conn, rows)
    with conn.cursor() as cur:
        cur.execute("select count(*) from visualizacao_agentes.agentes"); print("linhas:", cur.fetchone()[0])
    conn.close()

if __name__ == "__main__":
    main()
