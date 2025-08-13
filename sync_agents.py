import os, time, requests, psycopg2
from psycopg2.extras import Json

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
BASE = "https://api.movidesk.com/public/v1"
CUSTOM_FIELD_SQUAD_ID = "222343"

def get_with_retry(url, params=None, tries=4):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                time.sleep(1.5 + i)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last = e
            if getattr(e, "response", None) is not None and e.response.status_code in (400, 404):
                raise
            time.sleep(1.2 * (i + 1))
    raise last

def ensure_structure(conn):
    ddl = """
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

def fetch_people_list():
    out, skip, page = [], 0, 200
    while True:
        p = {"token": TOKEN, "$select": "id,businessName,userName,isActive,profileType,accessProfile", "$top": page, "$skip": skip}
        data = get_with_retry(f"{BASE}/persons", p)
        if not data:
            break
        out.extend(data)
        if len(data) < page:
            break
        skip += page
    return out

def fetch_person_detail(pid):
    p = {"token": TOKEN, "$expand": "teams,customFieldValues,emails"}
    return get_with_retry(f"{BASE}/persons/{pid}", p)

def extract_teams(detail):
    res = []
    arr = detail.get("teams") or []
    for it in arr:
        n = None
        if isinstance(it, dict):
            n = it.get("name")
            if not n and isinstance(it.get("team"), dict):
                n = it["team"].get("name")
        if n:
            res.append(str(n))
    return res

def extract_time_squad(detail):
    for cf in detail.get("customFieldValues") or []:
        if str(cf.get("customFieldId")) == CUSTOM_FIELD_SQUAD_ID:
            return cf.get("valueName") or cf.get("value")
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
        people = [p for p in fetch_people_list() if p.get("profileType") in (1, 3)]
        with conn.cursor() as cur:
            for p in people:
                pid = p.get("id")
                try:
                    pid_int = int(pid)
                except Exception:
                    continue
                detail = None
                teams, team_primary, time_squad = None, None, None
                try:
                    detail = fetch_person_detail(pid)
                    tnames = extract_teams(detail)
                    teams = tnames if tnames else None
                    team_primary = tnames[0] if tnames else None
                    time_squad = extract_time_squad(detail)
                except requests.HTTPError as e:
                    teams = None
                    team_primary = None
                    time_squad = None
                row = {
                    "agent_id": pid_int,
                    "name": (p.get("businessName") or "").strip(),
                    "email": (p.get("userName") or "").strip(),
                    "team_primary": team_primary,
                    "teams": teams,
                    "access_type": p.get("accessProfile"),
                    "is_active": bool(p.get("isActive")),
                    "raw": detail or p,
                    "time_squad": time_squad
                }
                cur.execute(UPSERT, {**row, "raw": Json(row["raw"])})
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
