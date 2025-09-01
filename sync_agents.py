import os
import json
import time
import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
FIELD_ID  = os.getenv("PERSON_TIME_SQUAD_FIELD_ID", "").strip()
FIELD_LBL = os.getenv("PERSON_TIME_SQUAD_FIELD_LABEL", "").strip()

def fetch_agents():
    assert API_TOKEN, "MOVIDESK_TOKEN ausente"
    url = f"{API_BASE}/persons"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))
    skip = 0
    filtro = os.getenv("MOVIDESK_PERSON_FILTER", "profileType ne 2")
    items = []
    while True:
        params = {"token": API_TOKEN, "$top": top, "$skip": skip, "$filter": filtro}
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

def _extract_email(p):
    email = (p.get("email") or p.get("emailAddress") or p.get("userName") or "")
    if email:
        return email.lower()
    emails = p.get("emails")
    if isinstance(emails, list) and emails:
        pref = next((e for e in emails if e.get("isDefault")), emails[0])
        return (pref.get("email") or "").lower() or None
    return None

def _extract_teams(p):
    team_primary = None
    names = []
    src = p.get("teams") or p.get("memberships") or []
    if isinstance(src, list):
        for t in src:
            if isinstance(t, str) and t:
                names.append(t)
            elif isinstance(t, dict):
                name = t.get("name") or (t.get("team") or {}).get("name") or t.get("teamName") or t.get("value")
                if name:
                    names.append(name)
                if (t.get("isDefault") or t.get("isPrimary") or t.get("default")) and name:
                    team_primary = name
    if not team_primary and names:
        team_primary = names[0]
    return (team_primary or None), (names or None)

def _extract_time_squad_from_custom_fields(p):
    cf = p.get("customFieldValues") or p.get("customFields") or []
    if not isinstance(cf, list):
        return None
    if FIELD_ID:
        for e in cf:
            try:
                if str(e.get("customFieldId","")).strip() == FIELD_ID:
                    return e.get("value") or e.get("text") or e.get("optionValue")
            except Exception:
                pass
    if FIELD_LBL:
        lbl = FIELD_LBL.lower()
        for e in cf:
            name = (e.get("field") or e.get("name") or e.get("label") or "")
            if isinstance(name, str) and name.lower() == lbl:
                return e.get("value") or e.get("text") or e.get("optionValue")
    return None

def _val(x):
    return x if x not in ("", [], {}) else None

def normalize(p):
    pid = p.get("id")
    try:
        agent_id = int(str(pid))
    except Exception:
        agent_id = pid
    name = p.get("businessName") or p.get("name")
    email = _extract_email(p)
    is_active = p.get("isActive")
    access_type = p.get("accessProfile") or p.get("profileType")
    team_primary, teams = _extract_teams(p)
    time_squad = _extract_time_squad_from_custom_fields(p)
    return {
        "agent_id": agent_id,
        "name": _val(name),
        "email": _val(email),
        "team_primary": _val(team_primary),
        "teams": teams,
        "access_type": _val(access_type),
        "is_active": bool(is_active) if is_active is not None else None,
        "raw": p,
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
          time_squad = COALESCE(EXCLUDED.time_squad, visualizacao_agentes.agentes.time_squad)
        """
        execute_values(cur, sql, values, template=template)

def main():
    people = fetch_agents()
    rows = [normalize(p) for p in people if isinstance(p, dict)]
    rows = [r for r in rows if r.get("agent_id")]
    if not rows:
        return
    batch = int(os.getenv("UPSERT_BATCH", "1000"))
    for i in range(0, len(rows), batch):
        upsert_agentes(rows[i:i+batch])

if __name__ == "__main__":
    main()
