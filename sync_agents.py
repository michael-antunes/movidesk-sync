import os, time, requests, psycopg2
from psycopg2.extras import Json

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
BASE = "https://api.movidesk.com/public/v1"
CUSTOM_FIELD_ID = "222343"

def get_with_retry(url, params=None, tries=4):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                time.sleep(2 + i)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last = e
            if getattr(e, "response", None) and e.response is not None and e.response.status_code in (400,404):
                raise
            time.sleep(1.2 * (i + 1))
    raise last

def ensure_structure(conn):
    ddl = """
    create schema if not exists visualizacao_agentes;

    create table if not exists visualizacao_agentes.agentes (
        agent_id    bigint primary key,
        name        text not null,
        email       text not null,
        team_primary text,
        teams        text[],
        access_type  text,
        is_active    boolean not null,
        raw          jsonb,
        updated_at   timestamp with time zone not null default now(),
        time_squad   text
    );

    do $$
    begin
      if not exists (select 1 from pg_indexes where schemaname='visualizacao_agentes' and indexname='idx_agentes_email')
      then create index idx_agentes_email on visualizacao_agentes.agentes (lower(email)); end if;

      if not exists (select 1 from pg_indexes where schemaname='visualizacao_agentes' and indexname='idx_agentes_team')
      then create index idx_agentes_team on visualizacao_agentes.agentes (team_primary); end if;

      if not exists (select 1 from pg_indexes where schemaname='visualizacao_agentes' and indexname='idx_agentes_teams_gin')
      then create index idx_agentes_teams_gin on visualizacao_agentes.agentes using gin (teams); end if;
    end$$;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def fetch_all_agents():
    out, skip, top = [], 0, 500
    while True:
        params = {
            "token": TOKEN,
            "$select": "id,businessName,userName,isActive,profileType,accessProfile",
            "$expand": "teams",
            "$top": top,
            "$skip": skip,
        }
        data = get_with_retry(f"{BASE}/persons", params)
        if not data:
            break
        out.extend(data)
        if len(data) < top:
            break
        skip += top
    return out

def parse_teams(p):
    names = []
    t = p.get("teams") or []
    for it in t:
        n = it.get("name")
        if n:
            names.append(str(n))
    return names

def person_time_squad(pid):
    d = get_with_retry(f"{BASE}/persons/{pid}", {"token": TOKEN, "$expand": "customFieldValues"})
    cf = d.get("customFieldValues") or []
    for item in cf:
        if str(item.get("customFieldId")) != CUSTOM_FIELD_ID:
            continue
        v = item.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            n = (v.get("name") or v.get("valueName") or v.get("value") or "").strip()
            if n:
                return n
        n2 = (item.get("name") or item.get("valueName") or "").strip()
        if n2:
            return n2
        items = item.get("items")
        if isinstance(items, list) and items:
            n3 = (items[0].get("name") or items[0].get("valueName") or items[0].get("value") or "").strip()
            if n3:
                return n3
    return None

UPSERT = """
insert into visualizacao_agentes.agentes
  (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at,time_squad)
values
  (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now(),%(time_squad)s)
on conflict (agent_id) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  is_active=excluded.is_active,
  raw=excluded.raw,
  updated_at=now(),
  time_squad=excluded.time_squad;
"""

def main():
    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)
        people = [p for p in fetch_all_agents() if p.get("profileType") in (1,3)]
        with conn.cursor() as cur:
            for p in people:
                pid = p.get("id")
                try:
                    pid_int = int(pid)
                except Exception:
                    continue
                teams = parse_teams(p)
                team_primary = teams[0] if teams else None
                email = (p.get("userName") or "").strip()
                time_squad = person_time_squad(pid)
                row = {
                    "agent_id": pid_int,
                    "name": (p.get("businessName") or "").strip(),
                    "email": email,
                    "team_primary": team_primary,
                    "teams": teams if teams else None,
                    "access_type": p.get("accessProfile"),
                    "is_active": bool(p.get("isActive")),
                    "raw": p,
                    "time_squad": time_squad,
                }
                cur.execute(UPSERT, {**row, "raw": Json(row["raw"])})
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
