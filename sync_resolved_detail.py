# -*- coding: utf-8 -*-
import os
import time
import logging
from typing import List, Dict, Any, Iterable
import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))
THROTTLE_SEC = float(os.getenv("THROTTLE_SEC", "0.25"))

def req(url: str, params: Dict[str, Any]) -> Any:
    all_params = {"token": API_TOKEN, **params}
    r = requests.get(url, params=all_params, timeout=60)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return None
    try:
        full = r.url.replace(API_TOKEN, "***")
    except Exception:
        full = url
    body = r.text[:5000]
    raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {full} - body: {body}", response=r)

def chunked(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def addcol(conn, col: str, typ: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            select 1
              from information_schema.columns
             where table_schema='visualizacao_resolvidos'
               and table_name='tickets_resolvidos'
               and column_name=%s
             limit 1
        """, (col,))
        if cur.fetchone():
            return
        cur.execute(f"alter table visualizacao_resolvidos.tickets_resolvidos add column {col} {typ}")
    conn.commit()

def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("""
            create table if not exists visualizacao_resolvidos.detail_control (
                ticket_id   integer primary key,
                last_update timestamptz default now()
            )
        """)
        cur.execute("""
            create table if not exists visualizacao_resolvidos.sync_control (
                name        text primary key,
                last_update timestamptz default now()
            )
        """)
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
              adicional_nome text
            )
        """)
    conn.commit()
    addcol(conn, "owner_id", "text")
    addcol(conn, "owner_name", "text")
    addcol(conn, "owner_team_name", "text")
    addcol(conn, "organization_id", "text")
    addcol(conn, "organization_name", "text")
    addcol(conn, "subject", "text")
    addcol(conn, "adicional_nome", "text")
    addcol(conn, "last_update", "timestamptz")

def get_audit_ids(conn, limit: int = 600) -> List[int]:
    with conn.cursor() as cur:
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
             group by ticket_id
             order by max(run_id) desc, ticket_id desc
             limit %s
        """, (limit,))
        rows = cur.fetchall() or []
    ids = [int(r[0]) for r in rows]
    logging.info("Audit pendentes: %d", len(ids))
    return ids

SELECT_FIELDS = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency",
    "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
    "subject",
    "ownerTeam"
])

EXPAND_EXPR = ",".join([
    "owner($select=id,businessName)",
    "clients($expand=organization($select=id,businessName))",
    "customFieldValues($select=customFieldId,value)"
])

def _fetch_ids_once(ids: List[int]) -> List[Dict[str, Any]]:
    filt = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND_EXPR,
        "$filter": filt,
        "$top": 100,
    }
    data = req(BASE_URL, params) or []
    time.sleep(THROTTLE_SEC)
    return data

def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    return _fetch_ids_once(ids)

def fetch_by_ids(all_ids: List[int], group_size: int = IDS_GROUP_SIZE) -> Iterable[List[Dict[str, Any]]]:
    for group in chunked(all_ids, group_size):
        yield fetch_group_by_ids(group)

def safe_get_owner(t: Dict[str, Any]) -> Dict[str, Any]:
    return (t.get("owner") or {}) if isinstance(t.get("owner"), dict) else {}

def safe_get_org(t: Dict[str, Any]) -> Dict[str, Any]:
    clients = t.get("clients") or []
    if clients and isinstance(clients, list):
        org = (clients[0] or {}).get("organization") or {}
        return org if isinstance(org, dict) else {}
    return {}

def extract_custom_29077(t: Dict[str, Any]) -> str:
    cfvals = t.get("customFieldValues") or []
    try:
        for cf in cfvals:
            cfid = cf.get("customFieldId")
            if cfid is None:
                continue
            if str(cfid) == "29077":
                val = cf.get("value")
                if val is None:
                    return None
                if isinstance(val, (dict, list)):
                    return str(val)
                return str(val)
    except Exception:
        pass
    return None

def map_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = safe_get_owner(t)
    org   = safe_get_org(t)
    owner_id   = owner.get("id")
    owner_name = owner.get("businessName")
    org_id   = org.get("id")
    org_name = org.get("businessName")
    row = {
        "ticket_id": int(t.get("id")),
        "status": t.get("status"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at": t.get("closedIn"),
        "last_cancelled_at": t.get("canceledIn"),
        "last_update": t.get("lastUpdate"),
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
        "adicional_nome": extract_custom_29077(t),
    }
    return row

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_name,organization_id,organization_name,subject,adicional_nome)
values %s
on conflict (ticket_id) do update set
 status               = excluded.status,
 last_resolved_at     = excluded.last_resolved_at,
 last_closed_at       = excluded.last_closed_at,
 last_cancelled_at    = excluded.last_cancelled_at,
 last_update          = excluded.last_update,
 origin               = excluded.origin,
 category             = excluded.category,
 urgency              = excluded.urgency,
 service_first_level  = excluded.service_first_level,
 service_second_level = excluded.service_second_level,
 service_third_level  = excluded.service_third_level,
 owner_id             = excluded.owner_id,
 owner_name           = excluded.owner_name,
 owner_team_name      = excluded.owner_team_name,
 organization_id      = excluded.organization_id,
 organization_name    = excluded.organization_name,
 subject              = excluded.subject,
 adicional_nome       = excluded.adicional_nome
"""

def upsert_rows(conn, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, [(
            r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"], r["last_cancelled_at"], r["last_update"],
            r["origin"], r["category"], r["urgency"], r["service_first_level"], r["service_second_level"], r["service_third_level"],
            r["owner_id"], r["owner_name"], r["owner_team_name"], r["organization_id"], r["organization_name"], r["subject"], r["adicional_nome"]
        ) for r in rows], page_size=200)
    conn.commit()

def clear_audit_for(conn, ticket_ids: List[int]) -> None:
    if not ticket_ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
               and ticket_id = any(%s)
        """, (ticket_ids,))
    conn.commit()

def mark_sync(conn, ticket_ids: List[int]) -> None:
    if not ticket_ids:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            insert into visualizacao_resolvidos.detail_control (ticket_id, last_update)
            values %s
            on conflict (ticket_id) do update set last_update = now()
        """, [(tid, None) for tid in ticket_ids])
        cur.execute("""
            insert into visualizacao_resolvidos.sync_control (name, last_update)
            values ('tickets_resolvidos', now())
            on conflict (name) do update set last_update = excluded.last_update
        """)
    conn.commit()

def main():
    with psycopg2.connect(NEON_DSN) as conn:
        ensure_schema(conn)
        audit_ids = get_audit_ids(conn, limit=600)
        if not audit_ids:
            logging.info("Nenhum ticket em audit_recent_missing para tickets_resolvidos. Nada a fazer.")
            return
        total_ok = 0
        processed_ids: List[int] = []
        for page in fetch_by_ids(audit_ids, group_size=IDS_GROUP_SIZE):
            if not page:
                continue
            rows = [map_ticket_row(t) for t in page if t and t.get("id")]
            if not rows:
                continue
            upsert_rows(conn, rows)
            ids_this = [r["ticket_id"] for r in rows]
            processed_ids.extend(ids_this)
            total_ok += len(rows)
        if processed_ids:
            mark_sync(conn, processed_ids)
            clear_audit_for(conn, processed_ids)
        logging.info("Upsert concluído. Registros gravados: %d (IDs distintos: %d)", total_ok, len(set(processed_ids)))

if __name__ == "__main__":
    main()
