import os, time, json, requests, psycopg2
from psycopg2.extras import Json

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
BASE = "https://api.movidesk.com/public/v1"
CUSTOM_FIELD_ID = 222343

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
            if hasattr(e, "response") and e.response is not None and e.response.status_code in (400,404):
                raise
            time.sleep(1.5 * (i + 1))
    raise last

def ensure_structure(conn):
    ddl = """
    create schema if not exists visualizacao_agentes;

    create table if not exists visualizacao_agentes.agentes (
        agent_id bigint primary key,
        name text not null,
        email text not null,
        team_primary text,
        teams text[],
        access_type text,
        is_active boolean not null,
        raw jsonb,
        updated_at timestamp with time zone not null default now(),
        time_squad text
    );

    do $$
    begin
      if not exists (
        select 1 from pg_indexes
        where schemaname='visualizacao_agentes' and indexname='idx_agentes_email'
      ) then
        create index idx_agentes_email on visualizacao_agentes.agentes (lower(email));
      end if;

      if not exists (
        select 1 from pg_indexes
        where schemaname='visualizacao_agentes' and indexname='idx_agentes_team'
      ) then
        create index idx_agentes_team on visualizacao_agentes.agentes (team_primary);
      end if;

      if not exists (
        select 1 from pg_indexes
        where schemaname='visualizacao_agentes' and indexname='idx_agentes_teams_gin'
      ) then
        create index idx_agentes_teams_gin on visualizacao_agentes.agentes using gin (teams);
      end if;
    end$$;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def list_people():
    out = []
    skip = 0
    top = 500
    while True:
        p = {
            "token": TOKEN,
            "$select": "id,businessName,userName,isActive,profileType,accessProfile",
            "$top": top,
            "$skip": skip,
        }
        data = get_with_retry(f"{BASE}/persons", p)
        if not data:
            break
        out.extend(data)
        if len(data) < top:
            break
        skip += top
    return out

def person_detail(pid):
    p = {
        "token": TOKEN,
        "$expand": "teams,customFieldValues,emails"
    }
    try:
        return get_with_retry(f"{BASE}/persons/{pid}", p)
    except requests.HTTPError as e:
        return None

def pick_email(detail, fallback):
    emails = detail.get("emails") or []
    chosen = None
    for e in emails:
        if e.get("isDefault"):
            chosen = e.get("email")
            break
    if not chosen and emails:
        chosen = emails[0].get("email")
    if not chosen:
        chosen = fallback or ""
    return chosen

def extract_time_squad(detail):
    cf = detail.get("customFieldValues") or []
    for item in cf:
        if str(item.get("customFieldId")) == str(CUSTOM_FIELD_ID):
            v = item.get("value")
            if isinstance(v, str) and v.strip():
                return v.strip()
            name = item.get("name") or item.get("valueName")
            if name:
                return str(name)
            items = item.get("items")
            if isinstance(items, list) and items:
                nm = items[0].get("name") or items[0].get("value")
                if nm:
                    return str(nm)
    return None

def upsert(conn, row):
    sql = """
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
    with conn.cursor() as cur:
        cur.execute(sql, {**row, "raw": Json(row["raw"])})

def main():
    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)
        people = [p for p in list_people() if p.get("profileType") in (1,3)]
        for p in people:
            pid = p.get("id")
            detail = person_detail(pid) or {}
            teams_detail = detail.get("teams") or []
            team_names = [t.get("name") for t in teams_detail if isinstance(t, dict) and t.get("name")]
            team_primary = team_names[0] if team_names else None
            email = pick_email(detail, p.get("userName"))
            row = {
                "agent_id": int(pid),
                "name": str(p.get("businessName") or "").strip(),
                "email": str(email or "").strip(),
                "team_primary": team_primary,
                "teams": team_names if team_names else None,
                "access_type": p.get("accessProfile"),
                "is_active": bool(p.get("isActive")),
                "raw": detail if detail else p,
                "time_squad": extract_time_squad(detail)
            }
            upsert(conn, row)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
