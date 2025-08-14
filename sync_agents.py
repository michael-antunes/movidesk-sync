import os
import time
import requests
import psycopg2
import psycopg2.extras as pgx

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
CUSTOM_FIELD_ID = int(os.getenv("MOVIDESK_CF_SQUAD", "222343"))
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))

def api_get(url, params):
    err = None
    for i in range(6):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code in (429,500,502,503,504):
                time.sleep(min(60,2**i)); continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            err = e; time.sleep(min(60,2**i))
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_team ON visualizacao_agentes.agentes (team_primary)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_teams_gin ON visualizacao_agentes.agentes USING GIN (teams)")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS visualizacao_agentes.agentes_historico (
            history_id    BIGSERIAL PRIMARY KEY,
            agent_id      BIGINT NOT NULL,
            change_date   DATE NOT NULL,
            changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            name          TEXT NOT NULL,
            email         TEXT NOT NULL,
            team_primary  TEXT,
            teams         TEXT[],
            access_type   TEXT,
            is_active     BOOLEAN NOT NULL,
            time_squad    TEXT,
            snapshot_hash TEXT NOT NULL
        )
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_agentes_hist_agent_date ON visualizacao_agentes.agentes_historico(agent_id, change_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_agentes_hist_agent ON visualizacao_agentes.agentes_historico(agent_id)")
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

def extract_time_squad(person, field_id):
    vals = person.get("customFieldValues") or []
    for cf in vals:
        try:
            if int(cf.get("customFieldId")) != int(field_id):
                continue
        except Exception:
            continue
        names = []
        items = cf.get("items") or []
        for it in items:
            if isinstance(it, dict):
                v = it.get("customFieldItem") or it.get("name") or it.get("text") or it.get("value")
            else:
                v = str(it)
            if isinstance(v, str) and v.strip():
                names.append(v.strip())
        v = cf.get("value")
        if isinstance(v, str) and v.strip():
            names.append(v.strip())
        if names:
            return ", ".join(dict.fromkeys(names))
        return None
    return None

def fetch_agents():
    params = {
        "token": TOKEN,
        "$select": "id,businessName,userName,isActive,profileType,accessProfile,teams",
        "$filter": "(profileType eq 1 or profileType eq 3)",
        "$expand": "customFieldValues($expand=items)",
        "$top": PAGE_SIZE,
        "$skip": 0
    }
    people = []
    while True:
        page = api_get(f"{BASE}/persons", params)
        if not isinstance(page, list) or not page:
            break
        people.extend(page)
        if len(page) < params["$top"]:
            break
        params["$skip"] += params["$top"]
    return people

def upsert_agents(conn, people):
    sql = """
    INSERT INTO visualizacao_agentes.agentes
    (agent_id,name,email,team_primary,teams,access_type,is_active,raw,updated_at,time_squad)
    VALUES
    (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,now(),%(time_squad)s)
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
        if p.get("profileType") not in (1,3,"1","3"):
            continue
        pid = p.get("id")
        try:
            pid = int(str(pid))
        except Exception:
            continue
        teams = extract_team_names(p)
        rows.append({
            "agent_id": pid,
            "name": p.get("businessName") or "",
            "email": p.get("userName") or "",
            "team_primary": teams[0] if teams else None,
            "teams": teams if teams else None,
            "access_type": p.get("accessProfile"),
            "is_active": bool(p.get("isActive")),
            "raw": pgx.Json(p),
            "time_squad": extract_time_squad(p, CUSTOM_FIELD_ID)
        })
    if not rows:
        return
    with conn.cursor() as cur:
        pgx.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()

def record_history(conn):
    with conn.cursor() as cur:
        cur.execute("""
        WITH nowsp AS (
          SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS d
        ),
        snap AS (
          SELECT
            a.agent_id,
            a.name,
            a.email,
            a.team_primary,
            a.teams,
            a.access_type,
            a.is_active,
            a.time_squad,
            md5(
              coalesce(lower(a.email),'')||'|'||
              coalesce(a.team_primary,'')||'|'||
              coalesce(array_to_string(a.teams, ','),'')||'|'||
              coalesce(a.time_squad,'')||'|'||
              CASE WHEN a.is_active THEN '1' ELSE '0' END
            ) AS h
          FROM visualizacao_agentes.agentes a
        ),
        last AS (
          SELECT DISTINCT ON (agent_id)
            agent_id, snapshot_hash
          FROM visualizacao_agentes.agentes_historico
          ORDER BY agent_id, changed_at DESC
        ),
        to_ins AS (
          SELECT s.*, n.d AS change_date
          FROM snap s
          CROSS JOIN nowsp n
          LEFT JOIN last l ON l.agent_id = s.agent_id
          WHERE coalesce(l.snapshot_hash, '') <> s.h
        )
        INSERT INTO visualizacao_agentes.agentes_historico
          (agent_id, change_date, name, email, team_primary, teams, access_type, is_active, time_squad, snapshot_hash)
        SELECT
          agent_id, change_date, name, email, team_primary, teams, access_type, is_active, time_squad, h
        FROM to_ins
        ON CONFLICT (agent_id, change_date) DO UPDATE SET
          name=excluded.name,
          email=excluded.email,
          team_primary=excluded.team_primary,
          teams=excluded.teams,
          access_type=excluded.access_type,
          is_active=excluded.is_active,
          time_squad=excluded.time_squad,
          snapshot_hash=excluded.snapshot_hash,
          changed_at=now()
        """)
    conn.commit()

def main():
    if not TOKEN or not DSN:
        raise RuntimeError("Defina as variáveis de ambiente MOVIDESK_TOKEN e NEON_DSN.")
    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)
        people = fetch_agents()
        upsert_agents(conn, people)
        record_history(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
