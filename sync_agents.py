import os
import time
import json
import requests
import psycopg2
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")

BASE = "https://api.movidesk.com/public/v1"

# ---------- HTTP helpers ----------
def get_with_retry(url, params, max_tries=6):
    for i in range(max_tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 2**i))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()

def fetch_paginated(endpoint, base_params=None, page_size=100):
    params = dict(base_params or {})
    params["token"] = API_TOKEN
    params["$top"]  = page_size
    items = []
    skip = 0
    while True:
        params["$skip"] = skip
        r = get_with_retry(f"{BASE}/{endpoint}", params)
        batch = r.json()
        if not isinstance(batch, list):
            batch = [batch] if batch else []
        items.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return items

# ---------- Movidesk fetch ----------
def fetch_agents():
    # tentamos pegar o máximo já expandido
    select = ",".join([
        "id","businessName","email","businessEmail",
        "isActive","profileType","businessProfile"
    ])
    expand = "teams($select=businessName)"
    params = {"$select": select, "$expand": expand}
    try:
        return fetch_paginated("agents", params, page_size=100)
    except requests.HTTPError:
        # fallback defensivo
        return fetch_paginated("agents", {}, page_size=100)

# ---------- Normalização ----------
def extract_email(a):
    return (
        a.get("email")
        or a.get("businessEmail")
        or (a.get("person") or {}).get("email")
        or ""
    )

def extract_access(a):
    return (
        a.get("profileType")
        or a.get("businessProfile")
        or (a.get("profile") or {}).get("type")
        or a.get("accessType")
        or ""
    )

def extract_active(a):
    v = a.get("isActive", a.get("active"))
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

def extract_teams(a):
    # pode vir lista de dicts, strings, etc.
    teams = a.get("teams") or a.get("groups") or []
    names = []
    for t in teams:
        if isinstance(t, dict):
            names.append(t.get("businessName") or t.get("name") or t.get("title"))
        elif isinstance(t, str):
            names.append(t)
    names = [n for n in names if n]
    return names or None

def normalize(agents_payload):
    out = []
    for a in agents_payload:
        aid = a.get("id") or a.get("agentId")
        if aid is None:
            continue
        try:
            aid = int(aid)
        except Exception:
            continue

        name = a.get("businessName") or a.get("name") or ""
        email = extract_email(a)
        acc   = extract_access(a)
        act   = extract_active(a)
        teams = extract_teams(a)
        team_primary = teams[0] if teams else None

        out.append({
            "agent_id": aid,
            "name": name,
            "email": email,
            "team_primary": team_primary,
            "teams": teams,
            "access_type": acc,
            "is_active": act,
            "raw": a
        })
    return out

# ---------- DB ----------
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
  name = excluded.name,
  email = excluded.email,
  team_primary = excluded.team_primary,
  teams = excluded.teams,
  access_type = excluded.access_type,
  is_active = excluded.is_active,
  raw = excluded.raw,
  updated_at = now();
"""

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def upsert(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        for r in rows:
            r = dict(r)
            r["raw"] = Json(r["raw"])  # jsonb
            cur.execute(UPSERT, r)
    conn.commit()

def main():
    print("🔄 Baixando agentes…")
    payload = fetch_agents()
    rows = normalize(payload)
    print(f"✅ {len(rows)} agentes normalizados.")
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    upsert(conn, rows)
    conn.close()
    print("🎉 Concluído.")

if __name__ == "__main__":
    main()
