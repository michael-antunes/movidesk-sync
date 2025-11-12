# sync_resolved_detail.py
import os, time, requests, psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

# Quantidade padrão de páginas * 100 para o fetch incremental por lastUpdate
TOP       = int(os.getenv("PAGES_UPSERT", "7")) * 100
THROTTLE  = float(os.getenv("THROTTLE_SEC", "0.5"))

# Limite de reprocessamento via auditoria por execução
AUDIT_LIMIT = int(os.getenv("AUDIT_LIMIT", "300"))

SCHEMA  = "visualizacao_resolvidos"
T_TICKETS = f"{SCHEMA}.tickets_resolvidos"
T_SYNC    = f"{SCHEMA}.sync_control"
T_AUDIT   = f"{SCHEMA}.audit_recent_missing"

AUDIT_TABLE_MATCHES = (
    "tickets_resolvidos",
    "visualizacao_resolvidos.tickets_resolvidos"
)

def conn():
    return psycopg2.connect(NEON_DSN)

def ensure_schema():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(f"create schema if not exists {SCHEMA}")
            # tabela principal
            cur.execute(f"""
            create table if not exists {T_TICKETS}(
              ticket_id integer primary key,
              status text,
              last_resolved_at timestamptz,
              last_closed_at   timestamptz,
              last_cancelled_at timestamptz,
              last_update timestamptz,
              origin text,
              category text,
              urgency text,
              service_first_level text,
              service_second_level text,
              service_third_level  text,
              owner_id text,
              owner_name text,
              organization_id text,
              organization_name text
            )
            """)
            # garantir colunas “soltas” se o banco já existia
            cur.execute(f"""
            do $$
            begin
              if not exists(select 1 from information_schema.columns
                  where table_schema=%s and table_name='tickets_resolvidos' and column_name='owner_id') then
                execute 'alter table {T_TICKETS} add column owner_id text';
              end if;
              if not exists(select 1 from information_schema.columns
                  where table_schema=%s and table_name='tickets_resolvidos' and column_name='owner_name') then
                execute 'alter table {T_TICKETS} add column owner_name text';
              end if;
              if not exists(select 1 from information_schema.columns
                  where table_schema=%s and table_name='tickets_resolvidos' and column_name='organization_id') then
                execute 'alter table {T_TICKETS} add column organization_id text';
              end if;
              if not exists(select 1 from information_schema.columns
                  where table_schema=%s and table_name='tickets_resolvidos' and column_name='organization_name') then
                execute 'alter table {T_TICKETS} add column organization_name text';
              end if;
            end$$
            """, (SCHEMA, SCHEMA, SCHEMA, SCHEMA))

            # controle
            cur.execute(f"""
            create table if not exists {T_SYNC}(
              name text primary key,
              last_update timestamptz default now(),
              last_index_run_at timestamptz,
              last_detail_run_at timestamptz
            )
            """)

            # auditoria (somente garante existência; já existe no seu projeto)
            cur.execute(f"""
            create table if not exists {T_AUDIT}(
              run_id    bigint not null,
              table_name text not null,
              ticket_id  integer not null
            )
            """)

            # índices úteis
            cur.execute(f"create index if not exists ix_tk_res_last_update on {T_TICKETS}(last_update)")
            cur.execute(f"create index if not exists ix_audit_tbl_ticket on {T_AUDIT}(table_name, ticket_id)")
    return True

UPSERT = f"""
insert into {T_TICKETS}
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,origin,category,urgency,
 service_first_level,service_second_level,service_third_level,owner_id,owner_name,organization_id,organization_name)
values (%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,%(origin)s,%(category)s,%(urgency)s,
        %(service_first_level)s,%(service_second_level)s,%(service_third_level)s,%(owner_id)s,%(owner_name)s,%(organization_id)s,%(organization_name)s)
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
 organization_name=excluded.organization_name
"""

SET_LASTRUN = f"""
insert into {T_SYNC}(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

GET_LASTRUN = f"select coalesce(max(last_detail_run_at), timestamp 'epoch') from {T_SYNC} where name='default'"

def req(url, params, retries=4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            # respeita rate-limit/intermitências
            sleep_s = float(r.headers.get("Retry-After") or 0) or (1.5 * (i + 1))
            time.sleep(sleep_s)
            continue
        r.raise_for_status()
    r.raise_for_status()

def to_utc(dt):
    if not dt:
        return None
    try:
        return datetime.fromisoformat(str(dt).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

SELECT_FIELDS = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
])
EXPAND_FIELDS = "owner,clients($expand=organization)"

def fetch_pages_since(since_iso):
    """Incremental por lastUpdate (resolvidos/fechados/cancelados)."""
    url = "https://api.movidesk.com/public/v1/tickets"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso
    skip = 0
    total = 0
    while True:
        page = req(url, {
            "token": API_TOKEN,
            "$select": SELECT_FIELDS,
            "$expand": EXPAND_FIELDS,
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

def fetch_by_ids(ids):
    """Reprocessa via auditoria: busca tickets por IDs específicos (chunk <= 50)."""
    if not ids:
        return
    url = "https://api.movidesk.com/public/v1/tickets"
    chunk = 50
    for i in range(0, len(ids), chunk):
        part = ids[i:i+chunk]
        filtro = " or ".join([f"id eq {int(x)}" for x in part])
        page = req(url, {
            "token": API_TOKEN,
            "$select": SELECT_FIELDS,
            "$expand": EXPAND_FIELDS,
            "$filter": filtro,
            "$top": 100
        }) or []
        if page:
            yield page
        time.sleep(THROTTLE)

def map_row(t):
    owner = t.get("owner") or {}
    owner_id = owner.get("id")
    owner_name = owner.get("businessName") or owner.get("fullName")
    org_id, org_name = None, None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
        org_id = org.get("id")
        org_name = org.get("businessName") or org.get("fullName")
    return {
        "ticket_id": t.get("id"),
        "status": t.get("status"),
        "last_resolved_at":  to_utc(t.get("resolvedIn")),
        "last_closed_at":    to_utc(t.get("closedIn")),
        "last_cancelled_at": to_utc(t.get("canceledIn")),
        "last_update":       to_utc(t.get("lastUpdate")),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
        "owner_id": owner_id,
        "owner_name": owner_name,
        "organization_id": org_id,
        "organization_name": org_name,
    }

def get_audit_ids(limit=AUDIT_LIMIT):
    """Busca IDs a reprocessar na auditoria para tickets_resolvidos."""
    sql = f"""
      select distinct ticket_id
      from {T_AUDIT}
      where table_name = any(%s)
      order by ticket_id desc
      limit %s
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (list(AUDIT_TABLE_MATCHES), limit))
            rows = cur.fetchall()
    return [r[0] for r in rows]

def clear_audit_ids(ids):
    if not ids: 
        return
    sql = f"""
      delete from {T_AUDIT}
      where table_name = any(%s)
        and ticket_id = any(%s)
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, (list(AUDIT_TABLE_MATCHES), list(ids)))

def main():
    ensure_schema()

    # 1) incremental por last_update desde a última execução
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(GET_LASTRUN)
            since = cur.fetchone()[0]

    if since == datetime(1970,1,1,tzinfo=timezone.utc):
        since = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")

    rows = []

    for page in fetch_pages_since(since_iso):
        for t in page:
            rows.append(map_row(t))

    # 2) reprocessa por auditoria (IDs marcados em visualizacao_resolvidos.audit_recent_missing)
    audit_ids = get_audit_ids(AUDIT_LIMIT)
    if audit_ids:
        seen = set()  # evita duplicar um mesmo ticket vindo do incremental
        for r in rows:
            seen.add(r["ticket_id"])
        ids_to_fetch = [i for i in audit_ids if i not in seen]
        for page in fetch_by_ids(ids_to_fetch):
            for t in page:
                rows.append(map_row(t))

    # 3) upsert
    if rows:
        with conn() as c:
            with c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=200)

    # 4) limpa a auditoria dos IDs efetivamente processados
    if audit_ids:
        clear_audit_ids(audit_ids)

    # 5) heartbeat (atualiza last_detail_run_at)
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
