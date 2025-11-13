# sync_resolved_detail.py
import os, time, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

TOP = int(os.getenv("PAGES_UPSERT", "7")) * 100
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.5"))

def conn():
    return psycopg2.connect(NEON_DSN)

def ensure_schema():
    with conn() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        # tabela base
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
        # garante colunas novas sem dropar nada
        for col, typ in [
            ("owner_id", "text"),
            ("owner_name", "text"),
            ("owner_team_id", "text"),
            ("owner_team_name", "text"),
            ("organization_id", "text"),
            ("organization_name", "text"),
        ]:
            cur.execute("""
            do $$
            begin
               if not exists(
                 select 1 from information_schema.columns
                 where table_schema='visualizacao_resolvidos'
                   and table_name='tickets_resolvidos'
                   and column_name=%s
               ) then
                 execute format('alter table visualizacao_resolvidos.tickets_resolvidos add column %I %s', %s, %s);
               end if;
            end$$
            """, (col, col, typ))

        # controle
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)

def req(url, params, retries=4, sleep_base=1.2):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(sleep_base * (i + 1))
            continue
        # erro definitivo
        raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}", response=r)
    # se saiu do loop
    r.raise_for_status()

def to_utc(dt):
    if not dt:
        return None
    try:
        return datetime.fromisoformat(str(dt).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_pages(since_iso):
    url = "https://api.movidesk.com/public/v1/tickets"
    # NÃO pedimos ownerTeam aqui — página fica estável e não quebra o OData
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso
    skip = 0
    total = 0
    while True:
        page = req(url, {
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
    owner = t.get("owner") or {}
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
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_team_id": None,       # preenchido no backfill
        "owner_team_name": None,     # preenchido no backfill
        "organization_id": org_id,
        "organization_name": org_name
    }

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
 owner_team_id=coalesce(excluded.owner_team_id, visualizacao_resolvidos.tickets_resolvidos.owner_team_id),
 owner_team_name=coalesce(excluded.owner_team_name, visualizacao_resolvidos.tickets_resolvidos.owner_team_name),
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = "select coalesce(max(last_detail_run_at), timestamp 'epoch') from visualizacao_resolvidos.sync_control where name='default'"

def fetch_owner_team_one(ticket_id: int):
    """
    Busca apenas a equipe do ticket. Tenta dois formatos aceitos pela API:
    1) $select=id,ownerTeam            (retorna objeto com id/name)
    2) $select=id & $expand=ownerTeam($select=id,name)
    """
    base = "https://api.movidesk.com/public/v1/tickets"
    # tentativa 1: só select
    params1 = {
        "token": API_TOKEN,
        "$filter": f"id eq {ticket_id}",
        "$select": "id,ownerTeam",
        "$top": 1
    }
    try:
        d = req(base, params1) or []
        t = d[0] if d else {}
        ot = t.get("ownerTeam") or {}
        if ot:
            return str(ot.get("id") or "") or None, (ot.get("name") or ot.get("businessName") or ot.get("fullName"))
    except requests.HTTPError:
        pass

    # tentativa 2: expand
    params2 = {
        "token": API_TOKEN,
        "$filter": f"id eq {ticket_id}",
        "$select": "id",
        "$expand": "ownerTeam($select=id,name)",
        "$top": 1
    }
    d = req(base, params2) or []
    t = d[0] if d else {}
    ot = t.get("ownerTeam") or {}
    if ot:
        return str(ot.get("id") or "") or None, (ot.get("name") or ot.get("businessName") or ot.get("fullName"))
    return None, None

def backfill_owner_team(since_dt_utc):
    """
    Preenche owner_team_* apenas para quem ainda está null.
    Limitamos por janela de tempo recente.
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where (owner_team_id is null or owner_team_id = '')
               and owner_id is not null
               and (last_update is null or last_update >= %s)
             order by ticket_id desc
             limit 500
        """, (since_dt_utc,))
        ids = [r[0] for r in cur.fetchall()]

    if not ids:
        return 0

    rows = []
    for tid in ids:
        try:
            team_id, team_name = fetch_owner_team_one(tid)
            if team_id or team_name:
                rows.append((team_id, team_name, tid))
            time.sleep(THROTTLE)
        except Exception:
            # não trava o lote por 1 falha
            continue

    if not rows:
        return 0

    with conn() as c, c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            update visualizacao_resolvidos.tickets_resolvidos
               set owner_team_id = %s,
                   owner_team_name = %s
             where ticket_id = %s
        """, rows, page_size=200)
    return len(rows)

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

    # backfill da equipe após upsert
    updated = backfill_owner_team(since)
    with conn() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)

    # log básico
    print(f"Detalhe: upsert={len(rows)} | owner_team backfilled={updated}")

if __name__ == "__main__":
    main()
