import os, time, json, requests, psycopg2
from datetime import date
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
BASE = "https://api.movidesk.com/public/v1"
CUSTOM_FIELD_ID = 222343

def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(min(60, 2**i)); continue
        r.raise_for_status(); return r
    r.raise_for_status()

def fetch_ids():
    p = {"token": API_TOKEN, "$select": "id,profileType", "$filter": "(profileType eq 1 or profileType eq 3)", "$top": 500, "$skip": 0}
    out = []
    while True:
        r = get_with_retry(f"{BASE}/persons", p)
        batch = r.json() or []
        if not batch: break
        out.extend([int(x["id"]) for x in batch if "id" in x])
        if len(batch) < p["$top"]: break
        p["$skip"] += p["$top"]
    return out

def person_detail(pid):
    sel = "id,businessName,userName,isActive,profileType,accessProfile,teams,emails,customFieldValues,additionalFields"
    opts = ["emails($select=email,isDefault,emailType),customFieldValues($select=customFieldId,items,value),additionalFields($select=id,name,items,value,values)",
            "emails($select=email,isDefault,emailType),customFieldValues($select=customFieldId,items,value)",
            "emails($select=email,isDefault,emailType),additionalFields($select=id,name,items,value,values)",
            "emails($select=email,isDefault,emailType)",
            ""]
    for ex in opts:
        params = {"token": API_TOKEN, "id": pid, "$select": sel}
        if ex: params["$expand"] = ex
        try:
            r = get_with_retry(f"{BASE}/persons", params)
            data = r.json()
            if isinstance(data, list) and data: data = data[0]
            return data
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                continue
            raise
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
    return p.get("userName") or ""

def extract_access(p):
    return p.get("accessProfile") or p.get("businessProfile") or (p.get("profile") or {}).get("type","") or ""

def extract_active(p):
    v = p.get("isActive", p.get("active"))
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

def pick_teams(p):
    raw = p.get("teams")
    if isinstance(raw, list):
        out = []
        for t in raw:
            if isinstance(t, dict):
                n = t.get("businessName") or t.get("name") or ""
            elif isinstance(t, str):
                n = t
            else:
                n = ""
            n = (n or "").strip()
            if n: out.append(n)
        return out or None
    return None

def extract_squad(p):
    cand = []
    for key in ("customFieldValues","additionalFields","personAdditionalFieldValues","fields"):
        v = p.get(key)
        if isinstance(v, list): cand.extend(v)
        elif isinstance(v, dict): cand.append(v)
    def from_item(it):
        ids = [it.get("id"), it.get("customFieldId")]
        names = [str(it.get("name") or it.get("title") or "").lower()]
        if CUSTOM_FIELD_ID in [x for x in ids if isinstance(x,int) or (isinstance(x,str) and x.isdigit())]:
            pass
        elif any("guerra de squad" in (n or "") for n in names):
            pass
        else:
            return None
        if isinstance(it.get("value"), str) and it.get("value").strip():
            return it.get("value").strip()
        items = it.get("items") or it.get("values")
        if isinstance(items, list) and items:
            for one in items:
                for k in ("value","name","businessName","title"):
                    val = one.get(k) if isinstance(one, dict) else None
                    if isinstance(val,str) and val.strip(): return val.strip()
        sel = it.get("selectedValue") or it.get("selected")
        if isinstance(sel,str) and sel.strip(): return sel.strip()
        return None
    for it in cand:
        try:
            v = from_item(it)
            if v: return v
        except Exception:
            continue
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
  squad        text,
  is_active    boolean not null,
  raw          jsonb,
  updated_at   timestamptz not null default now()
);
alter table visualizacao_agentes.agentes add column if not exists squad text;
create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));
create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);
create table if not exists visualizacao_agentes.agentes_historico (
  agent_id     bigint not null,
  snapshot_date date not null,
  name         text not null,
  email        text not null,
  team_primary text,
  teams        text[],
  access_type  text,
  squad        text,
  is_active    boolean not null,
  raw          jsonb,
  imported_at  timestamptz not null default now(),
  primary key (agent_id, snapshot_date)
);
create index if not exists idx_agentes_hist_email on visualizacao_agentes.agentes_historico (lower(email));
create index if not exists idx_agentes_hist_team on visualizacao_agentes.agentes_historico (team_primary);
create or replace view visualizacao_agentes.vw_agentes_ultimo as
select distinct on (agent_id) *
from visualizacao_agentes.agentes_historico
order by agent_id, snapshot_date desc;
"""

UPSERT_LATEST = """
insert into visualizacao_agentes.agentes
  (agent_id,name,email,team_primary,teams,access_type,squad,is_active,raw,updated_at)
values
  (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(squad)s,%(is_active)s,%(raw)s,now())
on conflict (agent_id) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  squad=excluded.squad,
  is_active=excluded.is_active,
  raw=excluded.raw,
  updated_at=now();
"""

UPSERT_HIST = """
insert into visualizacao_agentes.agentes_historico
  (agent_id,snapshot_date,name,email,team_primary,teams,access_type,squad,is_active,raw)
values
  (%(agent_id)s,%(snapshot_date)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(squad)s,%(is_active)s,%(raw)s)
on conflict (agent_id, snapshot_date) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  squad=excluded.squad,
  is_active=excluded.is_active,
  raw=excluded.raw,
  imported_at=now();
"""

def ensure_structure(conn):
    with conn.cursor() as cur: cur.execute(DDL)
    conn.commit()

def load_last_snapshots(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select distinct on (agent_id)
                   agent_id, snapshot_date, name, email, team_primary, teams, access_type, squad, is_active
            from visualizacao_agentes.agentes_historico
            order by agent_id,
