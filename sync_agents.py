import os, time, random, json, requests, psycopg2
from psycopg2.extras import Json
from zoneinfo import ZoneInfo
from datetime import datetime

API_TOKEN = os.getenv("MOVIDESK_TOKEN", "")
DSN = os.getenv("NEON_DSN", "")
BASE = "https://api.movidesk.com/public/v1"
CF_SQUAD_ID = os.getenv("MOVIDESK_SQUAD_FIELD_ID", "")
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "120"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
MAX_TRIES = int(os.getenv("MOVIDESK_MAX_TRIES", "8"))
PAGE_DELAY_MS = int(os.getenv("MOVIDESK_PAGE_DELAY_MS", "250"))

_session = requests.Session()

def get_with_retry(url, params):
    for i in range(MAX_TRIES):
        try:
            r = _session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            r.raise_for_status()
            return r
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            if isinstance(e, requests.exceptions.HTTPError):
                c = getattr(e.response, "status_code", 0) or 0
                if c not in (429,500,502,503,504):
                    raise
            if i == MAX_TRIES - 1:
                raise
            time.sleep(min(60, 1.7**i) + random.uniform(0,0.6))

def fetch_paginated(endpoint, params=None, page_size=PAGE_SIZE):
    p = dict(params or {})
    p["token"] = API_TOKEN
    p["$top"] = page_size
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
        if PAGE_DELAY_MS > 0:
            time.sleep(PAGE_DELAY_MS/1000.0)
    return items

def fetch_agents():
    select_fields = ["id","businessName","userName","isActive","profileType","accessProfile","emails","teams","customFieldValues"]
    params = {"$select": ",".join(select_fields)}
    return fetch_paginated("persons", params, page_size=PAGE_SIZE)

def pick_email(p):
    emails = p.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"):
                return (e.get("email") or "").strip()
        pref = ("professional","profissional","commercial","comercial")
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType","")).lower() in pref:
                return (e.get("email") or "").strip()
        f = emails[0]
        if isinstance(f, dict):
            return (f.get("email") or "").strip()
        if isinstance(f, str):
            return f.strip()
    return (p.get("userName") or "").strip()

def extract_access(p):
    return str(p.get("accessProfile") or p.get("businessProfile") or (p.get("profile") or {}).get("type","") or "")

def extract_active(p):
    v = p.get("isActive", p.get("active"))
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true","1","yes","y","sim")
    return bool(v)

def pick_teams(p):
    t = p.get("teams")
    if isinstance(t, list):
        out = []
        for x in t:
            if isinstance(x, dict):
                n = (x.get("businessName") or x.get("name") or "").strip()
                if n:
                    out.append(n)
            elif isinstance(x, str):
                s = x.strip()
                if s:
                    out.append(s)
        return out or None
    return None

def extract_squad(p):
    keys = ("customFieldValues","additionalFields","additionalFieldValues","customFields","personFields")
    for key in keys:
        arr = p.get(key)
        if not isinstance(arr, list):
            continue
        for it in arr:
            if not isinstance(it, dict):
                continue
            fid = str(it.get("id") or it.get("fieldId") or it.get("customFieldId") or "")
            if CF_SQUAD_ID and fid == CF_SQUAD_ID:
                val = it.get("value")
                if isinstance(val, dict):
                    return str(val.get("name") or val.get("value") or "").strip()
                return str(val or "").strip()
            name = str(it.get("label") or it.get("name") or "").lower()
            if "guerra" in name and "squad" in name:
                val = it.get("value")
                if isinstance(val, dict):
                    return str(val.get("name") or val.get("value") or "").strip()
                return str(val or "").strip()
    return None

DDL = """
create schema if not exists visualizacao_agentes;
create table if not exists visualizacao_agentes.agentes (
  agent_id     bigint primary key,
  name         text not null,
  email        text not null,
  team_primary text,
  teams        text[],
  access_type  text,
  time_squad   text,
  is_active    boolean not null,
  raw          jsonb,
  updated_at   timestamptz not null default now()
);
create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));
create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);
create table if not exists visualizacao_agentes.agentes_historico (
  agent_id     bigint not null,
  dia          date not null,
  name         text not null,
  email        text not null,
  team_primary text,
  teams        text[],
  access_type  text,
  time_squad   text,
  is_active    boolean not null,
  raw          jsonb,
  updated_at   timestamptz not null default now(),
  constraint agentes_historico_pkey primary key (agent_id, dia)
);
create index if not exists idx_ag_hist_email on visualizacao_agentes.agentes_histor_
