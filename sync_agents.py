import os, time, requests
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import Json

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]
BASE = "https://api.movidesk.com/public/v1"
SYNC_KEY = "agentes_lastsync"
CF_ID = 222343

def ts_utc(dt):
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_with_retry(url, params, tries=3, timeout=60):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            last = e
            if i < tries-1: time.sleep(2*(i+1))
    raise last

def ensure_struct(conn):
    ddl = """
    CREATE SCHEMA IF NOT EXISTS visualizacao_agentes;

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
    );

    CREATE TABLE IF NOT EXISTS visualizacao_agentes.sync_control (
        name TEXT PRIMARY KEY,
        last_update TIMESTAMPTZ NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_agentes_email ON visualizacao_agentes.agentes (lower(email));
    CREATE INDEX IF NOT EXISTS idx_agentes_team ON visualizacao_agentes.agentes (team_primary);
    CREATE INDEX IF NOT EXISTS idx_agentes_teams_gin ON visualizacao_agentes.agentes USING GIN (teams);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def get_last_sync(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT last_update FROM visualizacao_agentes.sync_control WHERE name=%s", (SYNC_KEY,))
        r = cur.fetchone()
    if r: return r[0]
    return datetime.now(timezone.utc) - timedelta(days=60)

def set_last_sync(conn, when):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO visualizacao_agentes.sync_control (name,last_update)
            VALUES (%s,%s)
            ON CONFLICT (name) DO UPDATE SET last_update = EXCLUDED.last_update
        """,(SYNC_KEY, ts_utc(when)))
    conn.commit()

def extract_time_squad(p):
    for key in ("customFieldValues","additionalFields","customFields"):
        arr = p.get(key)
        if not arr: continue
        for it in arr:
            cid = it.get("customFieldId") or it.get("id") or (it.get("customField") or {}).get("id")
            nm  = it.get("customFieldName") or it.get("name") or (it.get("customField") or {}).get("name")
            if cid == CF_ID or (nm and nm.lower().startswith("time (guerra de squad")):
                val = it.get("value") or it.get("formattedValue") or it.get("text") or it.get("title")
                if isinstance(val, list): return ",".join([str(x) for x in val])
                return str(val) if val is not None else None
    return None

def fetch_all_agents():
    expansions = ["teams,customFieldValues","teams,additionalFields","teams"]
    for exp in expansions:
        try:
            out = []
            page = 0
            size = 500
            while True:
                p = {
                    "token": TOKEN,
                    "$select": "id,businessName,userName,isActive,profileType,accessProfile",
                    "$expand": exp,
                    "$top": size,
                    "$skip": page*size
                }
                r = get_with_retry(f"{BASE}/persons", p)
                batch = r.json()
                out.extend(batch)
                if len(batch) < size: break
                page += 1
            return out
        except requests.exceptions.HTTPError:
            if exp == expansions[-1]: raise
            time.sleep(1)
    return []

def shape(row):
    teams = row.get("teams") or []
    team_primary = teams[0] if teams else None
    email = (row.get("userName") or "").strip()
    name = (row.get("businessName") or "").strip()
    access = row.get("accessProfile")
    is_active = bool(row.get("isActive"))
    time_squad = extract_time_squad(row)
    return {
        "agent_id": int(row["id"]),
        "name": name,
        "email": email,
        "team_primary": team_primary,
        "teams": teams if isinstance(teams, list) else [],
        "access_type": access,
        "is_active": is_active,
        "time_squad": time_squad,
        "raw": row
    }

def upsert_agents(conn, rows):
    sql = """
    INSERT INTO visualizacao_agentes.agentes
      (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at,time_squad)
    VALUES
      (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now(),%(time_squad)s)
    ON CONFLICT (agent_id) DO UPDATE SET
      name = EXCLUDED.name,
      email = EXCLUDED.email,
      team_primary = EXCLUDED.team_primary,
      teams = EXCLUDED.teams,
      access_type = EXCLUDED.access_type,
      is_active = EXCLUDED.is_active,
      raw = EXCLUDED.raw,
      updated_at = now(),
      time_squad = EXCLUDED.time_squad
    WHERE
      visualizacao_agentes.agentes.name IS DISTINCT FROM EXCLUDED.name OR
      visualizacao_agentes.agentes.email IS DISTINCT FROM EXCLUDED.email OR
      visualizacao_agentes.agentes.team_primary IS DISTINCT FROM EXCLUDED.team_primary OR
      visualizacao_agentes.agentes.teams IS DISTINCT FROM EXCLUDED.teams OR
      visualizacao_agentes.agentes.access_type IS DISTINCT FROM EXCLUDED.access_type OR
      visualizacao_agentes.agentes.is_active IS DISTINCT FROM EXCLUDED.is_active OR
      visualizacao_agentes.agentes.time_squad IS DISTINCT FROM EXCLUDED.time_squad OR
      visualizacao_agentes.agentes.raw IS DISTINCT FROM EXCLUDED.raw
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, {**r, "raw": Json(r["raw"])})
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    try:
        ensure_struct(conn)
        fetch_all_agents()  # warm-up DNS if needed
        people = [p for p in fetch_all_agents() if p.get("profileType") in (1,3)]
        shaped = [shape(x) for x in people]
        if shaped:
            upsert_agents(conn, shaped)
        set_last_sync(conn, datetime.now(timezone.utc))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
