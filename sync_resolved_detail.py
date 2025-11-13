# -*- coding: utf-8 -*-
import os, time, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

TOP = int(os.getenv("PAGES_UPSERT", "7")) * 100
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.5"))

BASE = "https://api.movidesk.com/public/v1"

def conn():
    return psycopg2.connect(NEON_DSN)

def ensure_schema():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute("create schema if not exists visualizacao_resolvidos")
            # tabela principal
            cur.execute("""
            create table if not exists visualizacao_resolvidos.tickets_resolvidos(
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
              organization_id text,
              organization_name text,
              owner_team_id text,
              owner_team_name text
            )
            """)
            # garante colunas novas sem derrubar nada
            cur.execute("""
            do $$
            begin
              if not exists(select 1 from information_schema.columns
                              where table_schema='visualizacao_resolvidos'
                                and table_name='tickets_resolvidos'
                                and column_name='owner_team_id') then
                alter table visualizacao_resolvidos.tickets_resolvidos
                  add column owner_team_id text;
              end if;

              if not exists(select 1 from information_schema.columns
                              where table_schema='visualizacao_resolvidos'
                                and table_name='tickets_resolvidos'
                                and column_name='owner_team_name') then
                alter table visualizacao_resolvidos.tickets_resolvidos
                  add column owner_team_name text;
              end if;
            end$$
            """)
            # controle
            cur.execute("""
            create table if not exists visualizacao_resolvidos.sync_control(
              name text primary key,
              last_update timestamptz default now(),
              last_index_run_at timestamptz,
              last_detail_run_at timestamptz
            )
            """)

UPSERT = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,origin,category,urgency,
 service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,organization_id,organization_name,
 owner_team_id,owner_team_name)
values (%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,%(origin)s,%(category)s,%(urgency)s,
        %(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
        %(owner_id)s,%(owner_name)s,%(organization_id)s,%(organization_name)s,
        %(owner_team_id)s,%(owner_team_name)s)
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
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name,
 owner_team_id=excluded.owner_team_id,
 owner_team_name=excluded.owner_team_name
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = "select coalesce(max(last_detail_run_at), timestamp 'epoch') from visualizacao_resolvidos.sync_control where name='default'"

def req(path_or_url: str, params: dict, is_full_url=False):
    """
    Chamada GET com tratamento de erro.
    Aceita url completa (is_full_url=True) ou path relativo (ex.: 'tickets').
    """
    if is_full_url:
        url = path_or_url + "?" + urlencode(params, safe="(),$= ")
    else:
        url = f"{BASE}/{path_or_url}?" + urlencode(params, safe="(),$= ")
    r = requests.get(url, timeout=60)
    if r.status_code == 200:
        return r.json()
    # deixa claro no log da Action
    raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}", response=r)

def to_utc(dt):
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z","+00:00")).astimezone(timezone.utc)
    except:
        return None

def fetch_pages(since_iso):
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso
    skip = 0
    total = 0
    while True:
        page = req("tickets", {
            "token":API_TOKEN,
            "$select":select_fields,
            "$expand":expand,
            "$filter":filtro,
            "$orderby":"lastUpdate asc",
            "$top": min(100, TOP - total),
            "$skip": skip
        }) or []
        if not page:
            break
        yield page
        got = len(page)
        total += got
        skip += got
        if total >= TOP or got < 100:
            break
        time.sleep(THROTTLE)

def map_row(t):
    owner = (t.get("owner") or {})
    owner_id = owner.get("id")
    owner_name = owner.get("businessName") or owner.get("fullName") or owner.get("name")

    org_id = None
    org_name = None
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
        "owner_id": str(owner_id) if owner_id is not None else None,
        "owner_name": owner_name,
        "organization_id": org_id,
        "organization_name": org_name,
        # preenchidos depois pelo enriquecimento
        "owner_team_id": None,
        "owner_team_name": None,
    }

def fetch_owner_teams(owner_ids):
    """
    Busca times dos owners no endpoint /persons.
    Retorna dict[str -> (team_id, team_name)].
    Faz várias tentativas de expand para cobrir variações do ambiente.
    """
    out = {}
    if not owner_ids:
        return out

    # monta filtro id eq A or id eq B ...
    filt = " or ".join([f"id eq {oid}" for oid in owner_ids])

    attempts = [
        # mais completo
        {"$select": "id", "$expand": "teams($select=id,name)"},
        # sem $select dentro do expand
        {"$select": "id", "$expand": "teams"},
        # só select id,teams (algumas instalações aceitam)
        {"$select": "id,teams"},
    ]

    for attempt in attempts:
        try:
            data = req("persons", {"token": API_TOKEN, "$filter": filt, **attempt})
        except Exception as e:
            # próxima tentativa
            continue
        for p in data or []:
            pid = str(p.get("id"))
            team_id = None
            team_name = None
            teams = p.get("teams")
            if isinstance(teams, list) and teams:
                t0 = teams[0] or {}
                # tenta várias chaves comuns
                team_id = t0.get("id") or t0.get("teamId") or t0.get("idTeam")
                team_name = t0.get("name") or t0.get("businessName") or t0.get("fullName")
            # salva se achou algo útil
            if pid and (team_id or team_name):
                out[pid] = (str(team_id) if team_id is not None else None, team_name)
        # se já preencheu pelo menos 1, encerra
        if out:
            break

    return out

def main():
    ensure_schema()

    # desde quando buscar
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(GET_LASTRUN)
            since = cur.fetchone()[0]
    if since == datetime(1970,1,1,tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")

    # coleta tickets
    rows = []
    for page in fetch_pages(since_iso):
        for t in page:
            rows.append(map_row(t))

    if rows:
        # -------- enriquecer com time do owner ----------
        owner_ids = sorted({r["owner_id"] for r in rows if r.get("owner_id")})
        teams = fetch_owner_teams(owner_ids)
        if teams:
            for r in rows:
                oid = r.get("owner_id")
                if oid and oid in teams:
                    r["owner_team_id"], r["owner_team_name"] = teams[oid]

        # grava
        with conn() as c:
            with c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=200)

    # marca last run
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
