import os
import json
import time
import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

def _load_json_env(name, default):
    v = os.getenv(name)
    if not v or not v.strip():
        return default
    try:
        return json.loads(v)
    except Exception:
        return default

SQUAD_EMAIL_MAP = _load_json_env("SQUAD_EMAIL_MAP", {})
SQUAD_TEAM_MAP = _load_json_env("SQUAD_TEAM_MAP", {})

def _compute_time_squad(email, team_primary, teams):
    if email and email in SQUAD_EMAIL_MAP:
        return SQUAD_EMAIL_MAP[email]
    if team_primary and team_primary in SQUAD_TEAM_MAP:
        return SQUAD_TEAM_MAP[team_primary]
    for t in teams or []:
        if t in SQUAD_TEAM_MAP:
            return SQUAD_TEAM_MAP[t]
    return None

def _val(x):
    return x if x not in ("", [], {}) else None

def fetch_agents():
    assert API_TOKEN, "MOVIDESK_TOKEN ausente"
    url = f"{API_BASE}/agents"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))
    skip = 0
    items = []
    while True:
        params = {"token": API_TOKEN, "$top": top, "$skip": skip}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        page = r.json() if r.text else []
        if not isinstance(page, list):
            page = []
        items.extend(page)
        if len(page) < top:
            break
        skip += top
        time.sleep(float(os.getenv("MOVIDESK_THROTTLE", "0.2")))
    return items

def normalize(item):
    agent_id = item.get("id") or item.get("agentId") or item.get("personId")
    name = item.get("businessName") or item.get("name")
    email = (item.get("email") or item.get("emailAddress") or "").lower() or None
    is_active = item.get("isActive")
    access_type = None
    if isinstance(item.get("businessProfile"), dict):
        access_type = item["businessProfile"].get("name")
    access_type = access_type or item.get("profileType") or item.get("accessType")
    team_primary = None
    tp = item.get("teamPrimary") or item.get("team_primary") or item.get("primaryTeam")
    if isinstance(tp, dict):
        team_primary = tp.get("name")
    elif isinstance(tp, str):
        team_primary = tp
    teams = []
    raw_teams = item.get("teams") or item.get("memberships") or []
    if isinstance(raw_teams, list):
        for t in raw_teams:
            if isinstance(t, dict):
                if "name" in t:
                    teams.append(t["name"])
                elif "team" in t and isinstance(t["team"], dict) and "name" in t["team"]:
                    teams.append(t["team"]["name"])
            elif isinstance(t, str):
                teams.append(t)
    team_primary = _val(team_primary)
    teams = teams or None
    time_squad = _compute_time_squad(email, team_primary, teams or [])
    return {
        "agent_id": agent_id,
        "name": _val(name),
        "email": _val(email),
        "team_primary": team_primary,
        "teams": teams,
        "access_type": _val(access_type),
        "is_active": bool(is_active) if is_active is not None else None,
        "raw": item,
        "time_squad": _val(time_squad),
    }

def upsert_agentes(rows):
    assert NEON_DSN, "NEON_DSN ausente"
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)"
    values = []
    for r in rows:
        values.append((
            r["agent_id"], r["name"], r["email"], r["team_primary"], r["teams"],
            r["access_type"], r["is_active"], Json(r["raw"]), r["time_squad"]
        ))
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        sql = """
        INSERT INTO visualizacao_agentes.agentes
          (agent_id, name, email, team_primary, teams, access_type, is_active, raw, updated_at, time_squad)
        VALUES %s
        ON CONFLICT (agent_id) DO UPDATE SET
          name = EXCLUDED.name,
          email = EXCLUDED.email,
          team_primary = EXCLUDED.team_primary,
          teams = EXCLUDED.teams,
          access_type = EXCLUDED.access_type,
          is_active = EXCLUDED.is_active,
          raw = EXCLUDED.raw,
          updated_at = NOW(),
          time_squad = EXCLUDED.time_squad
        """
        execute_values(cur, sql, values, template=template)

def main():
    data = fetch_agents()
    rows = [normalize(it) for it in data if isinstance(it, dict)]
    rows = [r for r in rows if r.get("agent_id")]
    if not rows:
        return
    batch = int(os.getenv("UPSERT_BATCH", "1000"))
    for i in range(0, len(rows), batch):
        upsert_agentes(rows[i:i+batch])

if __name__ == "__main__":
    main()
