import os, time, json, requests, psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def fetch_agents_list():
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile"]
    params = {"$select": ",".join(select_fields), "$expand": "emails($select=email,isDefault,emailType)"}
    data = fetch_paginated("persons", params, page_size=200)
    return data

def fetch_person_detail(agent_id):
    params = {
        "id": agent_id,
        "$select": "id",
        "$expand": "teams($select=businessName)",
        "token": API_TOKEN
    }
    r = get_with_retry(f"{BASE}/persons", params)
    j = r.json()
    if isinstance(j, list) and j: j = j[0]
    if isinstance(j, dict): return j
    return {}

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

def pick_teams_from_obj(obj):
    teams = obj.get("teams") or []
    out = []
    for t in teams:
        if isinstance(t, dict): out.append(t.get("businessName") or t.get("name") or t.get("title"))
        elif isinstance(t, str): out.append(t)
    out = [x for x in out if x]
    return out or None

def enrich_missing_teams(people):
    need = []
    by_id = {}
    for p in people:
        try: pid = int(p.get("id"))
        except: continue
        by_id[pid] = p
        t = pick_teams_from_obj(p)
        if not t: need.append(pid)
    if not need: return people
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_person_detail, aid): aid for aid in need}
        for f in as_completed(futs):
            aid = futs[f]
            try:
                d = f.result()
                t = pick_teams_from_obj(d)
                if t: results[aid] = t
            except Exception:
                pass
            time.sleep(0.05)
    for aid, teams in results.items():
        if aid in by_id:
            by_id[aid]["teams"] = [{"businessName": n} for n in teams]
    return list(by_id.values())

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
    people = fetch_agents_list()
    people = [p for p in people if p.get("profileType") in (1,3)]
    people = enrich_missing_teams(people)
    rows = []
    for p in people:
        try: pid = int(p.get("id"))
        except: continue
        teams = pick_teams_from_obj(p)
        email = pick_email(p)
        rows.append({
            "agent_id": pid,
            "name": p.get("businessName") or p.get("name") or "",
            "email": email,
            "team_primary": (teams or [None])[0],
            "teams": teams,
            "access_type": extract_access(p),
            "is_active": extract_active(p),
            "raw": p
        })
    upsert(conn, rows)
    with conn.cursor() as cur:
        cur.execute("select count(*) from visualizacao_agentes.agentes"); print("linhas:", cur.fetchone()[0])
    conn.close()

if __name__ == "__main__":
    main()
