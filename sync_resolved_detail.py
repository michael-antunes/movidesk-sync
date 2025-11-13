# sync_resolved_detail.py
# -*- coding: utf-8 -*-
import os, time, logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Dict, Any
import requests, psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"

# Controle/segurança
THROTTLE_SEC       = float(os.getenv("THROTTLE_SEC", "0.4"))
PAGES_UPSERT       = int(os.getenv("PAGES_UPSERT", "7"))          # páginas no modo incremental
OdataGroupMaxIDs   = int(os.getenv("ODATA_GROUP_MAX_IDS", "15"))  # grupo seguro p/ $filter (limite de 'node count 100')

def db():
    return psycopg2.connect(NEON_DSN)

def ensure_schema():
    with db() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("""
        create table if not exists visualizacao_resolvidos.tickets_resolvidos(
          ticket_id integer primary key,
          status text not null,
          last_resolved_at timestamptz,
          last_closed_at  timestamptz,
          last_cancelled_at timestamptz,
          last_update timestamptz,
          origin text,
          category text,
          urgency text,
          service_first_level text,
          service_second_level text,
          service_third_level text,
          owner_id   text,
          owner_name text,
          organization_id   text,
          organization_name text,
          owner_team_name   text
        )""")
        # Tabelas auxiliares já existentes no seu ambiente (mantemos):
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          window_start timestamptz,
          window_end   timestamptz,
          total_api    integer,
          missing_total integer,
          run_at timestamptz,
          window_from timestamptz,
          window_to   timestamptz,
          total_local integer,
          notes text
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint not null references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
          table_name text not null,
          ticket_id integer not null,
          constraint ux_audit_recent_missing unique (run_id, table_name, ticket_id)
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_ticket_watch(
          ticket_id integer primary key,
          hit_count integer not null default 1,
          last_reason text,
          last_seen_at timestamptz default now()
        )
        """)
    return True

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,organization_id,organization_name,owner_team_name)
values %s
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
 owner_team_name=excluded.owner_team_name
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('detail',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = """
select coalesce(max(last_detail_run_at), timestamp 'epoch')
from visualizacao_resolvidos.sync_control
where name='detail'
"""

def req(url: str, params: dict) -> Any:
    p = {"token": API_TOKEN, **params}
    r = requests.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json()
    raise requests.HTTPError(
        f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}",
        response=r
    )

def z(dtstr: str):
    if not dtstr:
        return None
    try:
        return datetime.fromisoformat(dtstr.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def chunked(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# ---------- FETCHERS ----------

SELECT_FIELDS = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
    "ownerTeam"  # Nome da equipe; API não expõe ownerTeamId
])
EXPAND = "owner($select=id,businessName,fullName),clients($expand=organization($select=id,businessName,fullName))"

def fetch_incremental_pages(since_iso: str):
    total = 0
    skip = 0
    while True:
        page = req(BASE_URL, {
            "$select": SELECT_FIELDS,
            "$expand": EXPAND,
            "$filter": "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso,
            "$orderby": "lastUpdate asc",
            "$top": 100,
            "$skip": skip
        }) or []
        if not page: break
        yield page
        got = len(page)
        total += got
        skip += got
        if got < 100 or total >= (PAGES_UPSERT * 100):
            break
        time.sleep(THROTTLE_SEC)

def fetch_group_by_ids(ids: List[int]):
    # Quebra em grupos para não estourar node-count 100
    for group in chunked(ids, OdataGroupMaxIDs):
        filt = " or ".join([f"id eq {i}" for i in group])
        data = req(BASE_URL, {
            "$select": SELECT_FIELDS,
            "$expand": EXPAND,
            "$filter": filt,
            "$top": 100
        }) or []
        yield data
        time.sleep(THROTTLE_SEC)

# ---------- MAP ----------

def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    # id de pessoa vem como string; normalizamos para texto
    owner_id   = (owner.get("id") or None)
    owner_name = (owner.get("businessName") or owner.get("fullName") or owner.get("name"))

    org_id = None
    org_nm = None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
        org_id = org.get("id")
        org_nm = org.get("businessName") or org.get("fullName") or org.get("name")

    return {
        "ticket_id": int(t.get("id")),
        "status": t.get("status"),
        "last_resolved_at": z(t.get("resolvedIn")),
        "last_closed_at":   z(t.get("closedIn")),
        "last_cancelled_at":z(t.get("canceledIn")),
        "last_update":      z(t.get("lastUpdate")),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency":  t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
        "owner_id": owner_id,
        "owner_name": owner_name,
        "organization_id": org_id,
        "organization_name": org_nm,
        "owner_team_name": t.get("ownerTeam")  # string ou None
    }

# ---------- MAIN ----------

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows: return 0
    with db() as c, c.cursor() as cur:
        execute_values(cur, UPSERT_SQL, [(
            r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"], r["last_cancelled_at"], r["last_update"],
            r["origin"], r["category"], r["urgency"], r["service_first_level"], r["service_second_level"], r["service_third_level"],
            r["owner_id"], r["owner_name"], r["organization_id"], r["organization_name"], r["owner_team_name"]
        ) for r in rows], page_size=200)
    return len(rows)

def purge_audit_missing(processed_ids: List[int], table_name: str):
    if not processed_ids: return
    with db() as c, c.cursor() as cur:
        # apaga do último RUN (ou de todos) as pendências resolvidas
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0]
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing
             where table_name = %s
               and run_id = %s
               and ticket_id = any(%s)
        """, (table_name, run_id, processed_ids))

def main():
    ensure_schema()

    # 1) IDs pendentes no audit (tabela 'tickets_resolvidos')
    with db() as c, c.cursor() as cur:
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
             order by ticket_id
             limit 600
        """)
        audit_ids = [r[0] for r in cur.fetchall()]

    logging.info("Audit pendentes: %d", len(audit_ids))

    processed_ids: List[int] = []

    # 2) Reprocessa pendentes por grupos
    if audit_ids:
        for page in fetch_group_by_ids(audit_ids):
            rows = [map_row(t) for t in page if t.get("id")]
            if not rows: 
                continue
            upsert_rows(rows)
            processed_ids.extend([r["ticket_id"] for r in rows])

        purge_audit_missing(processed_ids, "tickets_resolvidos")

    # 3) Incremental “desde o último run”
    with db() as c, c.cursor() as cur:
        cur.execute(GET_LASTRUN)
        since = cur.fetchone()[0]
    if since == datetime(1970,1,1,tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")

    inc_rows: List[Dict[str,Any]] = []
    for page in fetch_incremental_pages(since_iso):
        for t in page:
            inc_rows.append(map_row(t))
    if inc_rows:
        upsert_rows(inc_rows)

    # 4) Marca last run
    with db() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
