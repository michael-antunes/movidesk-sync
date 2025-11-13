# sync_resolved_detail.py
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import requests
import psycopg2
import psycopg2.extras

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
    with conn() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
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
              owner_team_id text,
              owner_team_name text,
              organization_id text,
              organization_name text
            )
        """)
        # garante colunas (sem DO/format)
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_id text")
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_name text")
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_team_id text")
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists owner_team_name text")
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists organization_id text")
        cur.execute("alter table visualizacao_resolvidos.tickets_resolvidos add column if not exists organization_name text")
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

GET_LASTRUN = """
select coalesce(max(last_detail_run_at), timestamp 'epoch')
from visualizacao_resolvidos.sync_control
where name='default'
"""


def req(url, params, retries=4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * (i + 1))
            continue
        raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}", response=r)
    raise requests.HTTPError("Falhou após tentativas")


def to_utc(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def fetch_pages(since_iso: str):
    """
    Busca páginas de tickets resolvidos/fechados/cancelados desde since_iso.
    NÃO seleciona 'ownerTeamId' (não existe no DTO de tickets).
    """
    url = f"{BASE}/tickets"

    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])

    # pega owner (p/ owner_id/name) e organization via clients
    expand = "owner,clients($expand=organization)"

    # DateTimeOffset sem aspas
    filtro = ("(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') "
              f"and lastUpdate ge {since_iso}")

    skip = 0
    total = 0
    while True:
        page = req(url, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$expand": expand,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
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


def fetch_owners_teams(owner_ids: List[str]) -> Dict[str, Tuple[str, str]]:
    """
    Resolve o time do responsável via /persons com $expand=teams.
    Retorna { owner_id: (team_id, team_name) } (pega o primeiro time se houver).
    """
    out: Dict[str, Tuple[str, str]] = {}
    if not owner_ids:
        return out

    # normaliza e remove None/vazios
    uniq = sorted({oid for oid in owner_ids if oid})

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    for ck in chunks(uniq, 40):
        f = " or ".join([f"id eq {oid}" for oid in ck])
        try:
            data = req(f"{BASE}/persons", {
                "token": API_TOKEN,
                "$select": "id",
                "$expand": "teams($select=id,name)",
                "$filter": f,
                "$top": len(ck)
            }) or []
            for p in data:
                pid = str(p.get("id"))
                teams = p.get("teams") or []
                if teams:
                    t0 = teams[0]
                    out[pid] = (str(t0.get("id")) if t0.get("id") is not None else None,
                                t0.get("name") or "")
        except Exception:
            # não faz hard-fail — só deixa sem time
            pass
        time.sleep(THROTTLE)
    return out


def map_row(t: dict, teams_by_owner: Dict[str, Tuple[str, str]]):
    owner = (t.get("owner") or {})
    owner_id = owner.get("id")
    owner_id_str = str(owner_id) if owner_id is not None else None
    owner_name = owner.get("businessName") or owner.get("fullName") or owner.get("name")

    team_id = None
    team_name = None
    if owner_id_str and owner_id_str in teams_by_owner:
        team_id, team_name = teams_by_owner[owner_id_str]

    org_id = None
    org_name = None
    clients = t.get("clients") or []
    if clients:
        org = (clients[0].get("organization") or {})
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
        "owner_id": owner_id_str,
        "owner_name": owner_name,
        "owner_team_id": team_id,
        "owner_team_name": team_name,
        "organization_id": org_id,
        "organization_name": org_name
    }


def main():
    ensure_schema()

    with conn() as c, c.cursor() as cur:
        cur.execute(GET_LASTRUN)
        since = cur.fetchone()[0]

    if since == datetime(1970, 1, 1, tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)

    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    raw_pages: List[dict] = []
    owner_ids: List[str] = []

    for page in fetch_pages(since_iso):
        raw_pages.extend(page)
        for t in page:
            oid = (t.get("owner") or {}).get("id")
            if oid is not None:
                owner_ids.append(str(oid))

    teams_by_owner = fetch_owners_teams(owner_ids)
    rows = [map_row(t, teams_by_owner) for t in raw_pages]

    if rows:
        with conn() as c, c.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=200)

    with conn() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)


if __name__ == "__main__":
    main()
