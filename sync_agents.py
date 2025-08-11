import os
import time
import json
import requests
import psycopg2
from psycopg2.extras import Json

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
BASE      = "https://api.movidesk.com/public/v1"

# ---------------- HTTP helpers ----------------
def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 2**i))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()

def fetch_paginated(endpoint, params=None, page_size=200):
    p = dict(params or {})
    p["token"] = API_TOKEN
    p["$top"]  = page_size
    items, skip = [], 0
    while True:
        p["$skip"] = skip
        r = get_with_retry(f"{BASE}/{endpoint}", p)
        batch = r.json() or []
        if not isinstance(batch, list):
            batch = [batch]
        items.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return items

# ---------------- Movidesk: PEOPLE API ----------------
def fetch_agents():
    """
    Agentes/colaboradores estão em /persons com profileType != 2 (2 = Cliente).
    """
    select_fields = [
        "id",
        "businessName",
        "userName",
        "isActive",
        "profileType",
        "accessProfile"   # pode vir vazio/padrão dependendo do tenant
    ]
    params = {
        "$select": ",".join(select_fields),
        "$expand": "emails,teams($select=businessName)",
        "$filter": "profileType ne 2"  # 1=Agente, 3=Agente+Cliente
    }
    return fetch_paginated("persons", params, page_size=200)

# ---------------- Normalização ----------------
def pick_email(person: dict) -> str:
    """
    emails: [{email, isDefault, emailType}]
    Preferência: padrão -> profissional/comercial -> primeiro.
    """
    emails = person.get("emails") or []
    if not isinstance(emails, list) or not emails:
        return ""
    for e in emails:
        if isinstance(e, dict) and e.get("isDefault"):
            return e.get("email") or ""
    for e in emails:
        if isinstance(e, dict) and str(e.get("emailType","")).lower() in ("professional","profissional","commercial","comercial"):
            return e.get("email") or ""
    first = emails[0]
    if isinstance(first, dict):
        return first.get("email") or ""
    if isinstance(first, str):
        return first
    return ""

def pick_teams(person: dict):
    teams = person.get("teams") or []
    names = []
    for t in teams:
        if isinstance(t, dict):
            names.append(t.get("businessName") or t.get("name") or t.get("title"))
        elif isinstance(t, str):
            names.append(t)
    names = [n for n in names if n]
    return names or None

def extract_access(person: dict) -> str:
    return (
        person.get("accessProfile")
        or person.get("businessProfile")
        or (person.get("profile") or {}).get("type", "")
        or ""
    )

def extract_active(person: dict) -> bool:
    v = person.get("isActive", person.get("active"))
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "sim")
    return bool(v)

def normalize(people):
    out = []
    for p in people:
        pid = p.get("id")
        if pid is None:
            continue
        try:
            pid = int(pid)
        except Exception:
            continue
