import os
import time
import requests
import psycopg2
import psycopg2.extras as pgx

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
CUSTOM_FIELD_ID = 222343

def get_with_retry(url, params, tries=5, timeout=60):
    err = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(60, 2**i))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            err = e
            time.sleep(min(60, 2**i))
    raise err

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS visualizacao_agentes")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS visualizacao_agentes.agentes (
            agent_id     BIGINT PRIMARY KEY,
            name         TEXT NOT NULL,
            email        TEXT NOT NULL,
            team_primary TEXT,
            teams        TEXT[],
            access_type  TEXT,
            is_active    BOOLEAN NOT NULL,
            raw          JSONB,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            time_squad   TEXT
        )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS agentes_pkey ON visualizacao_agentes.agentes(agent_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_email ON visualizacao_agentes.agentes (lower(email))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_team  ON visualizacao_agentes.agentes (team_primary)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_teams_gin ON visualizacao_agentes.agentes USING GIN (teams)")
    conn.commit()

def extract_team_names(person):
    t = person.get("teams")
    out = []
    if isinstance(t, list):
        for item in t:
            if isinstance(item, str) and item:
                out.append(item)
            elif isinstance(item, dict):
                n = item.get("name") or item.get("teamName") or item.get("value") or item.get("text")
                if isinstance(n, str) and n:
                    out.append(n)
    seen = set()
    uniq = []
    for n in out:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    return uniq

def extract_custom_field_value(person, field_id):
    vals = person.get("customFieldValues") or []
    if not isinstance(vals, list):
        return None
    for cf in vals:
        try:
            if int(cf.get("customFieldId")) != int(field_id):
                continue
        except Exception:
            continue
        v = cf.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
        items = cf.get("items")
        if isinstance(items, list) and items:
            names = []
            for it in items:
                name = None
                if isinstance(it, dict):
                    name = it.get("customFieldItem") or it.get("name") or it.get("text") or it.get("value")
                elif isinstance(it, str):
                    name = it
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
            if names:
                return ", ".join(dict.fromkeys(names))
        return None
    return None

def fetch_person_detail(pid):
    params = {"token": TOKEN, "$expand": "customFieldValues"}
    r = get_with_retry(f"{BASE}/persons/{pid}", params)
    return r.json()

def fetch_all_persons():
    params = {
        "token": TOKEN,
        "$select": "id,businessName,userName,isActive,profileType,accessProfile,teams",
        "$expand": "customFieldValues",
        "$top": 500,
        "$skip": 0
    }
    people = []
    while True:
        r = get_with_retry(f"{BASE}/persons", params)
        page = r.json()
        if not isinstance(page, list) or not page:
            break
        people.extend(page)
        if len(page) < params["$top"]:
            break
        params["$skip"] += params["$top"]
    missing = []
    for p in people:
        if extract_custom_field_value(p, CUSTOM_FIELD_ID) is None:
            missing.append(p.get("id"))
    for pid in missing:
        try:
            dp = fetch_person_detail(pid)
            for i in range(len(people)):
                if people[i].get("id") == pid:
                    people[i] = dp
                    break
        except Exception:
            continue
    return people

def upsert_agents(conn, people):
    sql = """
    INSERT INTO visualizacao_agentes.agentes 
    (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at,time_squad) 
    VALUES 
    (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now(),%(time_squad)s) 
    ON CONFLICT (agent_id) DO UPDATE SET 
        name        = EXCLUDED.name, 
        email       = EXCLUDED.email, 
        team_primary= EXCLUDED.team_primary, 
        teams       = EXCLUDED.teams, 
        access_type = EXCLUDED.access_type, 
        is_active   = EXCLUDED.is_active, 
        raw         = EXCLUDED.raw, 
        updated_at  = now(), 
        time_squad  = EXCLUDED.time_squad
    """
    rows = []
    for p in people:
        if p.get("profileType") not in (1, 3):
            continue
        pid = p.get("id")
        try:
            pid = int(str(pid))
        except Exception:
            continue
        teams = extract_team_names(p)
        time_squad = extract_custom_field_value(p, CUSTOM_FIELD_ID)
        rows.append({
            "agent_id": pid,
            "name": p.get("businessName") or "",
            "email": p.get("userName") or "",
            "team_primary": teams[0] if teams else None,
            "teams": teams if teams else None,
            "access_type": p.get("accessProfile"),
            "is_active": bool(p.get("isActive")),
            "raw": pgx.Json(p),
            "time_squad": time_squad
        })
    if not rows:
        return
    with conn.cursor() as cur:
        pgx.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()

def main():
    if not TOKEN or not DSN:
        raise RuntimeError("Defina as variáveis de ambiente MOVIDESK_TOKEN e NEON_DSN.")
    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)
        people = fetch_all_persons()
        upsert_agents(conn, people)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
