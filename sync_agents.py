import os
import json
import time
import requests
import psycopg2
import psycopg2.extras as pgx

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")

def get_with_retry(url, params, tries=3, timeout=60):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            last = e
            if e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                time.sleep(2)
                continue
            raise
        except requests.RequestException as e:
            last = e
            time.sleep(2)
    if last:
        raise last

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS visualizacao_agentes")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS visualizacao_agentes.agentes (
            agent_id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            team_primary TEXT,
            teams TEXT[],
            access_type TEXT,
            is_active BOOLEAN NOT NULL,
            raw JSONB,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            time_squad TEXT
        )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS agentes_pkey ON visualizacao_agentes.agentes(agent_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_email ON visualizacao_agentes.agentes (lower(email))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_team ON visualizacao_agentes.agentes (team_primary)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_teams_gin ON visualizacao_agentes.agentes USING GIN (teams)")
    conn.commit()

def extract_team_names(person):
    names = []
    t = person.get("teams")
    if isinstance(t, list):
        for item in t:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                n = item.get("name") or (item.get("team") or {}).get("name") or item.get("teamName") or item.get("value") or item.get("text")
                if isinstance(n, str):
                    names.append(n)
    seen = set()
    out = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out

def fetch_person_pages(expand_value):
    params = {
        "token": TOKEN,
        "$select": "id,businessName,userName,isActive,profileType,accessProfile",
        "$top": 500,
        "$skip": 0
    }
    if expand_value:
        params["$expand"] = expand_value
    while True:
        r = get_with_retry(f"{BASE}/persons", params)
        data = r.json()
        if not isinstance(data, list):
            break
        if not data:
            break
        for p in data:
            yield p
        params["$skip"] += params["$top"]

def fetch_all_agents():
    attempts = ["teams($select=name)", "teams", None]
    last_error = None
    for exp in attempts:
        try:
            return list(fetch_person_pages(exp))
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                last_error = e
                continue
            raise
    if last_error:
        raise last_error

def upsert_agents(conn, people):
    sql = """
    INSERT INTO visualizacao_agentes.agentes
    (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at,time_squad)
    VALUES (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now(),%(time_squad)s)
    ON CONFLICT (agent_id) DO UPDATE SET
        name=EXCLUDED.name,
        email=EXCLUDED.email,
        team_primary=EXCLUDED.team_primary,
        teams=EXCLUDED.teams,
        access_type=EXCLUDED.access_type,
        is_active=EXCLUDED.is_active,
        raw=EXCLUDED.raw,
        updated_at=now(),
        time_squad=EXCLUDED.time_squad
    """
    rows = []
    for p in people:
        pid = p.get("id")
        try:
            pid_num = int(str(pid))
        except Exception:
            continue
        name = p.get("businessName") or ""
        email = p.get("userName") or ""
        teams = extract_team_names(p)
        team_primary = teams[0] if teams else None
        access_type = p.get("accessProfile")
        is_active = bool(p.get("isActive"))
        rows.append({
            "agent_id": pid_num,
            "name": name,
            "email": email,
            "team_primary": team_primary,
            "teams": rows_cast_array(teams),
            "access_type": access_type,
            "is_active": is_active,
            "raw": pgx.Json(p),
            "time_squad": None
        })
    with conn.cursor() as cur:
        pgx.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()

def rows_cast_array(v):
    if v is None:
        return None
    return v

def main():
    if not TOKEN or not DSN:
        raise RuntimeError("Env MOVIDESK_TOKEN e NEON_DSN são obrigatórios")
    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)
        people = [p for p in fetch_all_agents() if p.get("profileType") in (1, 3)]
        upsert_agents(conn, people)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
