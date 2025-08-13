import os, time, requests, psycopg2
from psycopg2.extras import Json

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
BASE = "https://api.movidesk.com/public/v1"

def get_with_retry(url, params):
    params = dict(params or {})
    params["token"] = TOKEN
    last = None
    for i in range(5):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code >= 500:
                last = Exception(f"HTTP {r.status_code}")
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2 ** i)
    raise last

def ensure_structure(conn):
    ddl = """
    create schema if not exists visualizacao_agentes;
    create table if not exists visualizacao_agentes.agentes(
        agent_id bigint primary key,
        name text not null,
        email text not null,
        team_primary text,
        teams text[],
        access_type text,
        is_active boolean not null,
        raw jsonb,
        updated_at timestamptz not null default now(),
        time_squad text
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def fetch_all_agents():
    out = []
    skip = 0
    while True:
        p = {
            "$select": "id,businessName,userName,isActive,profileType,accessProfile",
            "$expand": "teams",
            "$top": 500,
            "$skip": skip
        }
        data = get_with_retry(f"{BASE}/persons", p)
        if not data:
            break
        out.extend(data)
        if len(data) < 500:
            break
        skip += 500
    return out

def parse_agent(row):
    agent_id = int(row["id"])
    name = row.get("businessName") or ""
    email = (row.get("userName") or "").lower()
    is_active = bool(row.get("isActive"))
    ap = row.get("accessProfile")
    access_type = ap.get("name") if isinstance(ap, dict) else ap
    teams = []
    team_primary = None
    t = row.get("teams")
    if isinstance(t, list):
        names = []
        for it in t:
            n = it.get("name") if isinstance(it, dict) else it
            if n:
                names.append(n)
        teams = names
        team_primary = names[0] if names else None
    return {
        "agent_id": agent_id,
        "name": name,
        "email": email,
        "is_active": is_active,
        "access_type": access_type,
        "teams": teams,
        "team_primary": team_primary,
        "raw": row
    }

def upsert_agent(conn, rec):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into visualizacao_agentes.agentes
            (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at)
            values (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now())
            on conflict (agent_id) do update set
                name = excluded.name,
                email = excluded.email,
                team_primary = excluded.team_primary,
                teams = excluded.teams,
                access_type = excluded.access_type,
                is_active = excluded.is_active,
                raw = excluded.raw,
                updated_at = now()
            """,
            {**rec, "raw": Json(rec["raw"])}
        )
    conn.commit()

def fetch_time_squad(person_id):
    detail = get_with_retry(f"{BASE}/persons/{person_id}", {"$expand": "customFieldValues"})
    cfs = detail.get("customFieldValues") or []
    for v in cfs:
        if str(v.get("customFieldId")) == "222343":
            val = v.get("value")
            if isinstance(val, dict):
                return val.get("label") or val.get("name") or val.get("value")
            return val
    return None

def update_time_squad(conn, agent_id, current_val):
    new_val = fetch_time_squad(agent_id)
    if new_val is None or new_val == current_val:
        return
    with conn.cursor() as cur:
        cur.execute(
            "update visualizacao_agentes.agentes set time_squad=%s, updated_at=now() where agent_id=%s",
            (new_val, agent_id),
        )
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    people = [p for p in fetch_all_agents() if p.get("profileType") in (1, 3)]
    for p in people:
        upsert_agent(conn, parse_agent(p))
    with conn.cursor() as cur:
        cur.execute("select agent_id, time_squad from visualizacao_agentes.agentes")
        rows = cur.fetchall()
    for agent_id, ts in rows:
        update_time_squad(conn, agent_id, ts)
    conn.close()

if __name__ == "__main__":
    main()
