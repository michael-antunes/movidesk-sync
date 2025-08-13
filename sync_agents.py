import os, time, json, requests, psycopg2
from datetime import datetime
from psycopg2.extras import Json
from zoneinfo import ZoneInfo

API_TOKEN = os.getenv("MOVIDESK_TOKEN", "")
DSN = os.getenv("NEON_DSN", "")
BASE = "https://api.movidesk.com/public/v1"
CF_SQUAD_ID = os.getenv("MOVIDESK_SQUAD_FIELD_ID", "222343")

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
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile","emails","teams","customFieldValues"]
    params = {"$select": ",".join(select_fields)}
    return fetch_paginated("persons", params, page_size=200)

def pick_email(p):
    emails = p.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"): return (e.get("email") or "").strip()
        pref = ("professional","profissional","commercial","comercial")
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType","")).lower() in pref: return (e.get("email") or "").strip()
        first = emails[0]
        if isinstance(first, dict): return (first.get("email") or "").strip()
        if isinstance(first, str): return first.strip()
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

def extract_squad(p):
    keys = ("customFieldValues","additionalFields","additionalFieldValues","customFields","personFields")
    for key in keys:
        arr = p.get(key)
        if not isinstance(arr, list): continue
        for it in arr:
            if not isinstance(it, dict): continue
            fid = str(it.get("id") or it.get("fieldId") or it.get("customFieldId") or "")
            if fid == str(CF_SQUAD_ID):
                val = it.get("value")
                if isinstance(val, dict): return str(val.get("name") or val.get("value") or "").strip()
                return str(val or "").strip()
            name = str(it.get("label") or it.get("name") or "").lower()
            if "guerra" in name and "squad" in name:
                val = it.get("value")
                if isinstance(val, dict): return str(val.get("name") or val.get("value") or "").strip()
                return str(val or "").strip()
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
  time_squad   text,
  is_active    boolean not null,
  raw          jsonb,
  updated_at   timestamptz not null default now()
);
create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));
create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);
create table if not exists visualizacao_agentes.agentes_historico (
  agent_id     bigint not null,
  dia          date not null,
  name         text not null,
  email        text not null,
  team_primary text,
  teams        text[],
  access_type  text,
  is_active    boolean not null,
  time_squad   text,
  raw          jsonb,
  updated_at   timestamptz not null default now(),
  constraint agentes_historico_pkey primary key (agent_id, dia)
);
create index if not exists idx_ag_hist_email on visualizacao_agentes.agentes_historico (lower(email));
create index if not exists idx_ag_hist_dia   on visualizacao_agentes.agentes_historico (dia);
"""

UPSERT_NOW = """
insert into visualizacao_agentes.agentes
  (agent_id,name,email,team_primary,teams,access_type,time_squad,is_active,raw,updated_at)
values
  (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(time_squad)s,%(is_active)s,%(raw)s,now())
on conflict (agent_id) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  time_squad=excluded.time_squad,
  is_active=excluded.is_active,
  raw=excluded.raw,
  updated_at=now();
"""

INSERT_HIST = """
insert into visualizacao_agentes.agentes_historico
  (agent_id,dia,name,email,team_primary,teams,access_type,is_active,time_squad,raw,updated_at)
values
  (%(agent_id)s,%(dia)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(time_squad)s,%(raw)s,now())
on conflict (agent_id,dia) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  is_active=excluded.is_active,
  time_squad=excluded.time_squad,
  raw=excluded.raw,
  updated_at=now();
"""

SELECT_LAST_HIST = """
select name,email,team_primary,teams,access_type,is_active,time_squad,dia
from visualizacao_agentes.agentes_historico
where agent_id=%s
order by dia desc
limit 1;
"""

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("alter table visualizacao_agentes.agentes add column if not exists time_squad text;")
        cur.execute("alter table visualizacao_agentes.agentes_historico add column if not exists dia date;")
        cur.execute("alter table visualizacao_agentes.agentes_historico add column if not exists time_squad text;")
        cur.execute("update visualizacao_agentes.agentes_historico set dia = current_date where dia is null;")
        cur.execute("alter table visualizacao_agentes.agentes_historico drop constraint if exists agentes_historico_pkey;")
        cur.execute("alter table visualizacao_agentes.agentes_historico add constraint agentes_historico_pkey primary key (agent_id, dia);")
    conn.commit()

def upsert_now(conn, rows):
    if not rows: return
    with conn.cursor() as cur:
        for r in rows: cur.execute(UPSERT_NOW, {**r, "raw": Json(r["raw"])})
    conn.commit()

def upsert_history(conn, row):
    with conn.cursor() as cur: cur.execute(INSERT_HIST, {**row, "raw": Json(row["raw"])})

def fetch_last_hist(conn, agent_id):
    with conn.cursor() as cur:
        cur.execute(SELECT_LAST_HIST, (agent_id,))
        r = cur.fetchone()
        if not r: return None
        return {"name": r[0], "email": r[1], "team_primary": r[2], "teams": r[3], "access_type": r[4], "is_active": r[5], "time_squad": r[6], "dia": r[7]}

def main():
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    people = [p for p in fetch_agents() if p.get("profileType") in (1,3)]
    rows_now = []
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    for p in people:
        try: pid = int(p.get("id"))
        except: continue
        teams = pick_teams(p)
        time_squad = extract_squad(p)
        row_now = {
            "agent_id": pid,
            "name": (p.get("businessName") or p.get("name") or "").strip(),
            "email": pick_email(p),
            "team_primary": (teams or [None])[0],
            "teams": teams,
            "access_type": extract_access(p),
            "time_squad": time_squad,
            "is_active": extract_active(p),
            "raw": p
        }
        rows_now.append(row_now)
    upsert_now(conn, rows_now)
    for r in rows_now:
        last = fetch_last_hist(conn, r["agent_id"])
        hist = {
            "agent_id": r["agent_id"],
            "dia": today,
            "name": r["name"],
            "email": r["email"],
            "team_primary": r["team_primary"],
            "teams": r["teams"],
            "access_type": r["access_type"],
            "is_active": r["is_active"],
            "time_squad": r["time_squad"],
            "raw": r["raw"]
        }
        must = False
        if last is None: must = True
        else:
            changed = (
                (last.get("name") or "") != (hist["name"] or "") or
                (last.get("email") or "") != (hist["email"] or "") or
                (last.get("team_primary") or "") != (hist["team_primary"] or "") or
                (last.get("access_type") or "") != (hist["access_type"] or "") or
                bool(last.get("is_active")) != bool(hist["is_active"]) or
                (last.get("time_squad") or "") != (hist["time_squad"] or "") or
                (last.get("teams") or []) != (hist["teams"] or [])
            )
            if changed or last.get("dia") != today: must = True
        if must: upsert_history(conn, hist)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
