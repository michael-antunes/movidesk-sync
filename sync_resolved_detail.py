# -*- coding: utf-8 -*-
import os
import time
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone

# --- Config ---
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

TOP = int(os.getenv("PAGES_UPSERT", "7")) * 100
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.5"))

# --- Conexão ---
def conn():
    return psycopg2.connect(NEON_DSN)

# --- Esquema / Tabelas ---
def ensure_schema():
    with conn() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")

        # Tabela final
        cur.execute("""
        create table if not exists visualizacao_resolvidos.tickets_resolvidos (
          ticket_id integer primary key,
          status text,
          last_resolved_at timestamptz,
          last_closed_at timestamptz,
          last_cancelled_at timestamptz,
          last_update timestamptz,
          origin text,
          category text,
          urgency text,
          service_first_level text,
          service_second_level text,
          service_third_level text,
          owner_id text,
          owner_name text,
          owner_team_id text,
          owner_team_name text,
          organization_id text,
          organization_name text
        )
        """)

        # Garante colunas (sem DO $$, sem placeholders)
        stmts = [
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists last_update timestamptz",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_id text",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_name text",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_team_id text",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_team_name text",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists organization_id text",
            "alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists organization_name text"
        ]
        for s in stmts:
            cur.execute(s)

        # Controle
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)

# --- Upsert / Controle ---
UPSERT = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_id,owner_team_name,organization_id,organization_name)
values (%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
        %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
        %(owner_id)s,%(owner_name)s,%(owner_team_id)s,%(owner_team_name)s,%(organization_id)s,%(organization_name)s)
on conflict (ticket_id) do update set
 status=excluded.status,
 last_resolved_at=excluded.last_resolved_at,
 last_closed_at=excluded.last_closed_at,
 last_cancelled_at=excluded.last_cancelled_at,
 last_update=excluded.last_update,
 origin=excluded.origin,
 category=excluded.category,
 urgency=excluded.urgency,
 service_first_level=excluded.service_first_level,
 service_second_level=excluded.service_second_level,
 service_third_level=excluded.service_third_level,
 owner_id=excluded.owner_id,
 owner_name=excluded.owner_name,
 owner_team_id=excluded.owner_team_id,
 owner_team_name=excluded.owner_team_name,
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = "select coalesce(max(last_detail_run_at), timestamp 'epoch') from visualizacao_resolvidos.sync_control where name='default'"

# --- HTTP util ---
def req(url, params, retries=4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.5*(i+1)); continue
        # 400 debug mais explícito
        raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}", response=r)
    r.raise_for_status()

def to_utc(dt):
    if not dt: return None
    try:
        return datetime.fromisoformat(dt.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

# --- Coleta ---
def fetch_pages(since_iso):
    url = "https://api.movidesk.com/public/v1/tickets"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
        "ownerTeam"  # <- somente o NOME da equipe
    ])
    # owner (person) e clients->organization para identificar a organização
    expand = "owner,clients($expand=organization)"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso

    total = 0
    skip = 0
    while True:
        top = min(100, TOP - total)
        if top <= 0:
            break
        page = req(url, {
            "token":API_TOKEN,
            "$select":select_fields,
            "$expand":expand,
            "$filter":filtro,
            "$orderby":"lastUpdate asc",
            "$top": top,
            "$skip": skip
        }) or []
        if not page: break
        yield page
        got = len(page)
        total += got
        skip += got
        if got < 100: break
        time.sleep(THROTTLE)

def map_row(t):
    # owner
    owner = t.get("owner") or {}
    owner_id = owner.get("id")
    owner_name = owner.get("businessName") or owner.get("fullName") or owner.get("name")

    # equipe (nome)
    owner_team_name = t.get("ownerTeam") or None
    owner_team_id = None  # a API /tickets não expõe o ID

    # organization (via clients[0].organization)
    org_id = None; org_name = None
    clients = t.get("clients") or []
    if clients:
        org = (clients[0] or {}).get("organization") or {}
        org_id = org.get("id")
        org_name = org.get("businessName") or org.get("fullName") or org.get("name")

    return {
        "ticket_id": t.get("id"),
        "status": t.get("status"),
        "last_resolved_at": to_utc(t.get("resolvedIn")),
        "last_closed_at": to_utc(t.get("closedIn")),
        "last_cancelled_at": to_utc(t.get("canceledIn")),
        "last_update": to_utc(t.get("lastUpdate")),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_team_id": owner_team_id,
        "owner_team_name": owner_team_name,
        "organization_id": org_id,
        "organization_name": org_name
    }

# --- Main ---
def main():
    ensure_schema()

    with conn() as c, c.cursor() as cur:
        cur.execute(GET_LASTRUN)
        since = cur.fetchone()[0]
    if since == datetime(1970,1,1,tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")

    rows = []
    for page in fetch_pages(since_iso):
        for t in page:
            rows.append(map_row(t))

    if rows:
        with conn() as c, c.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=200)

    with conn() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
