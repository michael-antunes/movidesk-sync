# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"

THROTTLE_SEC       = float(os.getenv("THROTTLE_SEC", "0.4"))
PAGES_UPSERT       = int(os.getenv("PAGES_UPSERT", "7"))        # 7*100 = até 700 no incremental
FILTER_GROUP_SIZE  = int(os.getenv("FILTER_GROUP_SIZE", "10"))  # ORs por requisição
MAX_IDS_PER_RUN    = int(os.getenv("MAX_IDS_PER_RUN", "1200"))

SELECT_FIELDS = ",".join([
    "id",
    "status",
    "resolvedIn",
    "closedIn",
    "canceledIn",
    "lastUpdate",
    "origin",
    "category",
    "urgency",
    "serviceFirstLevel",
    "serviceSecondLevel",
    "serviceThirdLevel",
    "subject",
    "ownerTeam",    # nome da equipe
    "customFields"  # para achar o adicional 29077
])

# Sem fullName para não quebrar; organization idem.
EXPAND = "owner($select=id,businessName),clients($expand=organization($select=id,businessName))"

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------
def conn():
    return psycopg2.connect(NEON_DSN)

def ensure_schema() -> None:
    """Cria schema/tabela e adiciona colunas novas se faltarem (sem DROPs)."""
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
          owner_team_name text,
          organization_id text,
          organization_name text,
          subject text,
          adicional_29077_nome text
        )
        """)

        def addcol(col: str, typ: str):
            # Usa EXECUTE format no lado do servidor (sem parâmetros do psycopg2).
            cur.execute(f"""
            do $$
            begin
              if not exists(
                select 1 from information_schema.columns
                 where table_schema='visualizacao_resolvidos'
                   and table_name='tickets_resolvidos'
                   and column_name='{col}'
              ) then
                execute format('alter table visualizacao_resolvidos.tickets_resolvidos add column %I {typ}', '{col}');
              end if;
            end$$
            """)

        # Garante colunas novas (idempotente)
        addcol("owner_team_name", "text")
        addcol("subject", "text")
        addcol("adicional_29077_nome", "text")
        addcol("owner_id", "text")
        addcol("owner_name", "text")
        addcol("organization_id", "text")
        addcol("organization_name", "text")

        # controle de execução
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_name,organization_id,organization_name,subject,adicional_29077_nome)
values (%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
        %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
        %(owner_id)s,%(owner_name)s,%(owner_team_name)s,%(organization_id)s,%(organization_name)s,%(subject)s,%(adicional_29077_nome)s)
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
 owner_team_name=excluded.owner_team_name,
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name,
 subject=excluded.subject,
 adicional_29077_nome=excluded.adicional_29077_nome
"""

GET_LASTRUN_SQL = """
select coalesce(max(last_detail_run_at), timestamp 'epoch')
from visualizacao_resolvidos.sync_control
where name='default'
"""

SET_LASTRUN_SQL = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('default',now(),now())
on conflict (name) do update set last_update=now(), last_detail_run_at=now()
"""

# ------------------------------------------------------------------------------
# HTTP helpers
# ------------------------------------------------------------------------------
def req(url: str, params: Dict[str, Any], retries: int = 3) -> Any:
    for i in range(retries):
        r = requests.get(url, params={"token": API_TOKEN, **params}, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.2 * (i + 1))
            continue
        body = ""
        try:
            body = r.text
        except Exception:
            pass
        raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {body}", response=r)
    r.raise_for_status()

# ------------------------------------------------------------------------------
# OData fetchers
# ------------------------------------------------------------------------------
def chunk(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _fetch_ids_once(ids: List[int]) -> List[Dict[str, Any]]:
    filt = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND,
        "$filter": filt
    }
    data = req(BASE_URL, params) or []
    time.sleep(THROTTLE_SEC)
    return data

def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    try:
        return _fetch_ids_once(ids)
    except requests.HTTPError as e:
        msg = (str(e) or "").lower()
        if "node count limit" in msg and len(ids) > 1:
            mid = len(ids) // 2
            left  = fetch_group_by_ids(ids[:mid])
            right = fetch_group_by_ids(ids[mid:])
            return (left or []) + (right or [])
        raise

def fetch_by_ids(all_ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    for group in chunk(all_ids, FILTER_GROUP_SIZE):
        yield fetch_group_by_ids(group)

def fetch_pages(since_iso: str) -> Iterable[List[Dict[str, Any]]]:
    top_total = PAGES_UPSERT * 100
    got_total = 0
    skip = 0
    while True:
        params = {
            "$select": SELECT_FIELDS,
            "$expand": EXPAND,
            "$filter": f"lastUpdate ge {since_iso} and (baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')",
            "$orderby": "lastUpdate asc",
            "$top": 100,
            "$skip": skip
        }
        page = req(BASE_URL, params) or []
        if not page:
            break
        yield page
        got = len(page)
        got_total += got
        skip += got
        if got < 100 or got_total >= top_total:
            break
        time.sleep(THROTTLE_SEC)

# ------------------------------------------------------------------------------
# Mapping helpers
# ------------------------------------------------------------------------------
def to_utc(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def extract_org(t: Dict[str, Any]) -> (Optional[str], Optional[str]):
    clients = t.get("clients") or []
    if not clients:
        return None, None
    org = (clients[0].get("organization") or {})
    return org.get("id"), (org.get("businessName") or org.get("fullName"))

def extract_owner(t: Dict[str, Any]) -> (Optional[str], Optional[str]):
    owner = t.get("owner") or {}
    return owner.get("id"), (owner.get("businessName") or owner.get("fullName"))

def extract_cf_nome_29077(t: Dict[str, Any]) -> Optional[str]:
    # tenta "29077" e "nome" no id; se não achar, apenas "29077"
    for cf in (t.get("customFields") or []):
        cid = (cf.get("id") or "")
        if "29077" in cid and "nome" in cid.lower():
            return cf.get("value")
    for cf in (t.get("customFields") or []):
        cid = (cf.get("id") or "")
        if "29077" in cid:
            return cf.get("value")
    return None

def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner_id, owner_name = extract_owner(t)
    org_id, org_name = extract_org(t)
    return {
        "ticket_id": int(t.get("id")),
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
        "owner_team_name": t.get("ownerTeam"),
        "organization_id": org_id,
        "organization_name": org_name,
        "subject": t.get("subject"),
        "adicional_29077_nome": extract_cf_nome_29077(t)
    }

# ------------------------------------------------------------------------------
# Audit helpers
# ------------------------------------------------------------------------------
def load_audit_ids(limit: int = MAX_IDS_PER_RUN) -> List[int]:
    with conn() as c, c.cursor() as cur:
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0]
        if not run_id:
            return []
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where run_id = %s
               and table_name = 'tickets_resolvidos'
             order by ticket_id
             limit %s
        """, (run_id, limit))
        ids = [r[0] for r in cur.fetchall()]
        logging.info("Audit pendentes: %d", len(ids))
        return ids

def clear_audit_for(ids: List[int]) -> None:
    if not ids:
        return
    with conn() as c, c.cursor() as cur:
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0]
        if not run_id:
            return
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing
             where run_id = %s
               and table_name = 'tickets_resolvidos'
               and ticket_id = any(%s)
        """, (run_id, ids))

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    ensure_schema()

    # 1) Prioriza o audit
    audit_ids = load_audit_ids()
    processed_any = False

    if audit_ids:
        rows: List[Dict[str, Any]] = []
        for page in fetch_by_ids(audit_ids):
            for t in (page or []):
                try:
                    rows.append(map_row(t))
                except Exception as e:
                    logging.warning("Falha map_row id=%s: %s", t.get("id"), e)
        if rows:
            with conn() as c, c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
            clear_audit_for([r["ticket_id"] for r in rows])
            processed_any = True
            logging.info("Upsert (audit): %d", len(rows))

    # 2) Fallback incremental
    if not processed_any:
        with conn() as c, c.cursor() as cur:
            cur.execute(GET_LASTRUN_SQL)
            since = cur.fetchone()[0]
        if since == datetime(1970, 1, 1, tzinfo=timezone.utc):
            since = datetime.now(timezone.utc) - timedelta(days=7)
        since_iso = since.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        logging.info("Incremental desde: %s", since_iso)

        inc_rows: List[Dict[str, Any]] = []
        for page in fetch_pages(since_iso):
            for t in page:
                try:
                    inc_rows.append(map_row(t))
                except Exception as e:
                    logging.warning("Falha map_row id=%s: %s", t.get("id"), e)
        if inc_rows:
            with conn() as c, c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, inc_rows, page_size=200)
            logging.info("Upsert (incremental): %d", len(inc_rows))

    # 3) Marca last run
    with conn() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN_SQL)

if __name__ == "__main__":
    main()
