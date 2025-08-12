import os
import time
import json
import hashlib
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from zoneinfo import ZoneInfo

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")
BASE = "https://api.movidesk.com/public/v1"

def get_with_retry(url, params, tries=6):
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 2 ** i))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()

def fetch_paginated(endpoint, params=None, page_size=200, hard_limit=20000):
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
        if len(batch) < page_size or len(items) >= hard_limit:
            break
        skip += page_size
    return items

def fetch_agents():
    select_fields = ["id", "businessName", "userName", "isActive", "profileType", "accessProfile"]
    params = {
        "$select": ",".join(select_fields),
        "$expand": "emails($select=email,isDefault,emailType),teams"
    }
    people = fetch_paginated("persons", params, page_size=200)
    people = [p for p in people if p.get("profileType") in (1, 3)]
    return people

def fetch_person_extra(pid):
    variants = ["additionalFields", "additionalFieldValues", "customFieldValues", "customFields"]
    for v in variants:
        try:
            p = {"token": API_TOKEN, "id": pid, "$select": "id", "$expand": v}
            r = get_with_retry(f"{BASE}/persons", p)
            return r.json() or {}
        except requests.exceptions.HTTPError:
            continue
    return {}

def pick_email(p):
    emails = p.get("emails") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if isinstance(e, dict) and e.get("isDefault"):
                return e.get("email") or ""
        for e in emails:
            if isinstance(e, dict) and str(e.get("emailType", "")).lower() in ("professional", "profissional", "commercial", "comercial"):
                return e.get("email") or ""
        first = emails[0]
        if isinstance(first, dict):
            return first.get("email") or ""
        if isinstance(first, str):
            return first
    return p.get("userName") or ""

def extract_access(p):
    return p.get("accessProfile") or p.get("businessProfile") or (p.get("profile") or {}).get("type", "") or ""

def extract_active(p):
    v = p.get("isActive", p.get("active"))
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "sim")
    return bool(v)

def pick_teams(p):
    t = p.get("teams")
    if isinstance(t, list):
        t = [x for x in t if x]
        return t or None
    return None

def search_time_squad(obj):
    target_ids = {"222343"}
    target_names = {"time (guerra de squads)", "time (guerra de squad)", "time guerra de squads"}
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            fid = str(cur.get("id") or cur.get("fieldId") or cur.get("customFieldId") or "")
            name = str(cur.get("name") or cur.get("businessName") or cur.get("alias") or cur.get("customField") or "").strip().lower()
            if fid in target_ids or name in target_names:
                for key in ("value", "option", "text", "businessName", "name"):
                    val = cur.get(key)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
                    if isinstance(val, dict):
                        for k2 in ("name", "businessName", "text", "value"):
                            vv = val.get(k2)
                            if isinstance(vv, str) and vv.strip():
                                return vv.strip()
                vs = cur.get("values") or cur.get("options")
                if isinstance(vs, list):
                    for it in vs:
                        if isinstance(it, str) and it.strip():
                            return it.strip()
                        if isinstance(it, dict):
                            for k2 in ("name", "businessName", "text", "value"):
                                vv = it.get(k2)
                                if isinstance(vv, str) and vv.strip():
                                    return vv.strip()
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

UPSERT_SNAPSHOT = """
insert into visualizacao_agentes.agentes
  (agent_id,name,email,team_primary,teams,access_type,time_squad,is_active,raw,updated_at)
values
  (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(time_squad)s,%(is_active)s,%(raw)s,now())
on conflict (agent_id) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  time_squad=excluded.time_squad,
  is_active=excluded.is_active,
  raw=excluded.raw,
  updated_at=now();
"""

UPSERT_HISTORY = """
insert into visualizacao_agentes.agentes_historico
  (agent_id,dia,name,email,team_primary,teams,access_type,time_squad,is_active,raw,imported_at)
values
  (%(agent_id)s,%(dia)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(time_squad)s,%(is_active)s,%(raw)s,now())
on conflict (agent_id,dia) do update set
  name=excluded.name,
  email=excluded.email,
  team_primary=excluded.team_primary,
  teams=excluded.teams,
  access_type=excluded.access_type,
  time_squad=excluded.time_squad,
  is_active=excluded.is_active,
  raw=excluded.raw,
  imported_at=now();
"""

def ensure_structure(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_agentes;")
        cur.execute("""
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
        """)
        cur.execute("alter table visualizacao_agentes.agentes add column if not exists time_squad text;")
        cur.execute("create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));")
        cur.execute("create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);")
        cur.execute("""
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
              imported_at  timestamptz not null default now(),
              primary key (agent_id, dia)
            );
        """)
        cur.execute("alter table visualizacao_agentes.agentes_historico add column if not exists dia date;")
        cur.execute("update visualizacao_agentes.agentes_historico set dia = current_date where dia is null;")
        cur.execute("alter table visualizacao_agentes.agentes_historico drop constraint if exists agentes_historico_pkey;")
        cur.execute("alter table visualizacao_agentes.agentes_historico add constraint agentes_historico_pkey primary key (agent_id, dia);")
        cur.execute("create index if not exists idx_ag_hist_email on visualizacao_agentes.agentes_historico (lower(email));")
        cur.execute("create index if not exists idx_ag_hist_day   on visualizacao_agentes.agentes_historico (dia);")
    conn.commit()

def latest_fingerprints(conn, agent_ids):
    if not agent_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct on (agent_id)
                   agent_id,
                   md5(
                       coalesce(name,'') || '|' ||
                       coalesce(email,'') || '|' ||
                       coalesce(team_primary,'') || '|' ||
                       coalesce(array_to_string(teams,','),'') || '|' ||
                       coalesce(access_type,'') || '|' ||
                       coalesce(time_squad,'') || '|' ||
                       is_active::text
                   ) as fp,
                   dia
            from visualizacao_agentes.agentes_historico
            where agent_id = any(%s)
            order by agent_id, dia desc
            """,
            (list(agent_ids),),
        )
        out = {}
        for aid, fp, dia in cur.fetchall():
            out[aid] = (fp, dia)
        return out

def fingerprint(row):
    base = "|".join([
        row.get("name") or "",
        row.get("email") or "",
        row.get("team_primary") or "",
        ",".join(row.get("teams") or []),
        row.get("access_type") or "",
        row.get("time_squad") or "",
        "1" if row.get("is_active") else "0",
    ])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def upsert_snapshot(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(UPSERT_SNAPSHOT, {**r, "raw": Json(r["raw"])})
    conn.commit()

def upsert_history_changed(conn, rows, dia_br):
    if not rows:
        return
    ids = {r["agent_id"] for r in rows}
    last = latest_fingerprints(conn, ids)
    to_write = []
    for r in rows:
        fp_now = fingerprint(r)
        prev = last.get(r["agent_id"])
        if prev is None or prev[0] != fp_now:
            to_write.append({**r, "dia": dia_br})
        else:
            if prev[1] == dia_br:
                to_write.append({**r, "dia": dia_br})
    if not to_write:
        return
    with conn.cursor() as cur:
        for r in to_write:
            cur.execute(UPSERT_HISTORY, {**r, "raw": Json(r["raw"])})
    conn.commit()

def main():
    tz = ZoneInfo("America/Sao_Paulo")
    dia_br = datetime.now(tz).date()
    conn = psycopg2.connect(DSN)
    ensure_structure(conn)
    people = fetch_agents()
    rows = []
    for p in people:
        try:
            pid = int(p.get("id"))
        except:
            continue
        teams = pick_teams(p)
        extra = fetch_person_extra(pid)
        time_squad = search_time_squad(extra) or search_time_squad(p) or None
        rows.append({
            "agent_id": pid,
            "name": p.get("businessName") or p.get("name") or "",
            "email": pick_email(p),
            "team_primary": (teams or [None])[0],
            "teams": teams,
            "access_type": extract_access(p),
            "time_squad": time_squad,
            "is_active": extract_active(p),
            "raw": p if extra in ({}, None) else {"base": p, "extra": extra}
        })
    upsert_snapshot(conn, rows)
    upsert_history_changed(conn, rows, dia_br)
    conn.close()

if __name__ == "__main__":
    main()
