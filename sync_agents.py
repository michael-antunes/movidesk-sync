import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1").rstrip("/")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
FIELD_ID = os.getenv("PERSON_TIME_SQUAD_FIELD_ID", "").strip()
FIELD_LBL = os.getenv("PERSON_TIME_SQUAD_FIELD_LABEL", "").strip()

def _sleep(seconds):
    time.sleep(float(seconds))

def _get(url, params):
    retries = int(os.getenv("MOVIDESK_RETRIES", "6"))
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            _sleep(float(ra) if ra and ra.replace(".", "", 1).isdigit() else 60.0)
            continue
        if 500 <= r.status_code < 600:
            _sleep(min(60.0, 2.0 ** i))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def fetch_agents():
    assert API_TOKEN, "MOVIDESK_TOKEN ausente"
    url = f"{API_BASE}/persons"
    top = int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))
    skip = 0
    filtro = os.getenv("MOVIDESK_PERSON_FILTER", "profileType ne 2")
    expand = os.getenv("MOVIDESK_PERSON_EXPAND", "emails").strip()
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "6.2"))
    items = []
    while True:
        params = {"token": API_TOKEN, "$top": top, "$skip": skip, "$filter": filtro}
        if expand:
            params["$expand"] = expand
        r = _get(url, params)
        page = r.json() if r.text else []
        if not isinstance(page, list):
            page = []
        items.extend(page)
        if len(page) < top:
            break
        skip += top
        _sleep(throttle)
    return items

def _pick_default_email(emails):
    if not isinstance(emails, list) or not emails:
        return None
    pref = next((e for e in emails if isinstance(e, dict) and e.get("isDefault") is True), None)
    e = pref or (emails[0] if isinstance(emails[0], dict) else None)
    if not e:
        return None
    v = (e.get("email") or "").strip().lower()
    return v or None

def _email_from_username_if_email(p):
    v = (p.get("userName") or "").strip().lower()
    if "@" in v and "." in v:
        return v
    return None

def _extract_email(p):
    v = _pick_default_email(p.get("emails"))
    if v:
        return v
    v = (p.get("email") or p.get("emailAddress") or "").strip().lower()
    if v:
        return v
    v = _email_from_username_if_email(p)
    if v:
        return v
    pid = p.get("id")
    try:
        agent_id = int(str(pid))
    except Exception:
        agent_id = str(pid) if pid is not None else "unknown"
    return f"noemail+{agent_id}@invalid.local"

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

def _pick_first_items_label(e):
    items = e.get("items")
    if isinstance(items, list) and items:
        for it in items:
            if isinstance(it, dict):
                v = it.get("customFieldItem") or it.get("text") or it.get("value")
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None

def _extract_time_squad_from_custom_fields(p):
    cf = p.get("customFieldValues") or p.get("customFields") or []
    if not isinstance(cf, list):
        return None
    if FIELD_ID:
        for e in cf:
            if str(e.get("customFieldId", "")).strip() == FIELD_ID:
                v = e.get("value") or e.get("text") or e.get("optionValue") or _pick_first_items_label(e)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    if FIELD_LBL:
        lbl = FIELD_LBL.lower()
        for e in cf:
            name = (e.get("field") or e.get("name") or e.get("label") or "")
            if isinstance(name, str) and name.lower() == lbl:
                v = e.get("value") or e.get("text") or e.get("optionValue") or _pick_first_items_label(e)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    for e in cf:
        v = _pick_first_items_label(e)
        if isinstance(v, str) and v.lower().startswith("time "):
            return v
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
    raw_full = p
    return {
        "agent_id": agent_id,
        "name": _val(name),
        "email": _val(email),
        "team_primary": _val(team_primary),
        "teams": teams,
        "access_type": _val(access_type),
        "is_active": bool(is_active) if is_active is not None else None,
        "raw_full": raw_full,
        "time_squad": _val(time_squad),
    }

def upsert_agentes(rows):
    assert NEON_DSN, "NEON_DSN ausente"
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)"
    values = []
    for r in rows:
        values.append((
            r["agent_id"], r["name"], r["email"], r["team_primary"], r["teams"],
            r["access_type"], r["is_active"], Json(r["raw_full"]), r["time_squad"]
        ))
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        sql = """
        INSERT INTO visualizacao_agentes.agentes
          (agent_id, name, email, team_primary, teams, access_type, is_active, raw_full, updated_at, time_squad)
        VALUES %s
        ON CONFLICT (agent_id) DO UPDATE SET
          name = EXCLUDED.name,
          email = EXCLUDED.email,
          team_primary = EXCLUDED.team_primary,
          teams = EXCLUDED.teams,
          access_type = EXCLUDED.access_type,
          is_active = EXCLUDED.is_active,
          raw_full = EXCLUDED.raw_full,
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
        upsert_agentes(rows[i:i + batch])

if __name__ == "__main__":
    main()
