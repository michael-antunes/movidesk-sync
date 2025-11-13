# -*- coding: utf-8 -*-
"""
sync_resolved_detail.py

- Reprocessa IDs de visualizacao_resolvidos.audit_recent_missing (table_name='tickets_resolvidos')
  do último run e grava/atualiza em visualizacao_resolvidos.tickets_resolvidos.
- Depois faz incremental por lastUpdate a partir de sync_control.last_detail_run_at.
- Campos de responsável:
    owner_id        -> owner.id
    owner_name      -> owner.businessName | owner.fullName | owner.name
    owner_team_name -> ownerTeam (string)
- Lotes pequenos para evitar "node count limit" do OData.
"""

import os
import time
import logging
from typing import Dict, Any, Iterable, List, Tuple
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
import psycopg2.extras

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

THROTTLE      = float(os.getenv("THROTTLE_SEC", "0.4"))
TOP_LIMIT     = 100
# Força lote bem pequeno; ainda existe fallback recursivo quando necessário.
IDS_PER_CALL  = int(os.getenv("IDS_PER_CALL", "8"))

BASE_URL = "https://api.movidesk.com/public/v1"

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------
def conn():
    return psycopg2.connect(NEON_DSN)

def ensure_schema() -> None:
    """Cria objetos mínimos; não derruba nada."""
    with conn() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute("""
        create table if not exists visualizacao_resolvidos.tickets_resolvidos(
            ticket_id            integer primary key,
            status               text,
            last_resolved_at     timestamptz,
            last_closed_at       timestamptz,
            last_cancelled_at    timestamptz,
            last_update          timestamptz,
            origin               text,
            category             text,
            urgency              text,
            service_first_level  text,
            service_second_level text,
            service_third_level  text,
            owner_id             text,
            owner_name           text,
            owner_team_name      text,
            organization_id      text,
            organization_name    text
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_index_run_at timestamptz,
          last_detail_run_at timestamptz
        )
        """)

# ------------------------------------------------------------------------------
# HTTP / OData helpers
# ------------------------------------------------------------------------------
def req(url: str, params: Dict[str, Any], retries: int = 4) -> Any:
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * (i + 1))
            continue
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}",
            response=r
        )
    r.raise_for_status()

def to_utc(dt_str: Any):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(str(dt_str).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

# ------------------------------------------------------------------------------
# Movidesk fetchers
# ------------------------------------------------------------------------------
SELECT_FIELDS = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency",
    "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
    "ownerTeam"  # string com o nome da equipe
])
EXPAND = "owner,clients($expand=organization)"

def fetch_pages_since(since_iso: str) -> Iterable[List[Dict[str, Any]]]:
    url = f"{BASE_URL}/tickets"
    filtro = "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')" \
             f" and lastUpdate ge {since_iso}"
    skip = 0
    while True:
        params = {
            "token": API_TOKEN,
            "$select": SELECT_FIELDS,
            "$expand": EXPAND,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
            "$top": TOP_LIMIT,
            "$skip": skip
        }
        page = req(url, params) or []
        if not page:
            break
        yield page
        if len(page) < TOP_LIMIT:
            break
        skip += len(page)
        time.sleep(THROTTLE)

def _fetch_group_ids(group: List[int]) -> Iterable[List[Dict[str, Any]]]:
    """
    Busca segura: se a API retornar 400/414 (node count/URI grande),
    divide o grupo ao meio e tenta novamente.
    """
    base = f"{BASE_URL}/tickets"
    if not group:
        return
    filt = " or ".join([f"id eq {i}" for i in group])
    params = {
        "token": API_TOKEN,
        "$select": SELECT_FIELDS,
        "$expand": EXPAND,
        "$filter": filt,
        "$top": TOP_LIMIT
    }
    try:
        page = req(base, params) or []
        yield page
        time.sleep(THROTTLE)
    except requests.HTTPError as e:
        status = getattr(e, "response", None).status_code if getattr(e, "response", None) else None
        msg = (str(e) or "").lower()
        # quando o OData reclama do tamanho do filtro/quantidade de nós
        if status in (400, 414) and ("node count" in msg or "uri" in msg or "$filter" in msg):
            if len(group) == 1:
                raise
            mid = max(1, len(group)//2)
            left, right = group[:mid], group[mid:]
            logging.info("Dividindo lote por limite OData: %s -> %s + %s", len(group), len(left), len(right))
            for page in _fetch_group_ids(left):
                yield page
            for page in _fetch_group_ids(right):
                yield page
        else:
            raise

def fetch_by_ids(ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    if not ids:
        return
    ids = sorted(set(int(x) for x in ids))
    # Força lotes pequenos para já evitar 400; _fetch_group_ids ainda divide se necessário.
    for i in range(0, len(ids), IDS_PER_CALL):
        group = ids[i:i+IDS_PER_CALL]
        for page in _fetch_group_ids(group):
            yield page

# ------------------------------------------------------------------------------
# Mapping + UPSERT
# ------------------------------------------------------------------------------
def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = (t.get("owner") or {})
    owner_id = owner.get("id")
    owner_name = owner.get("businessName") or owner.get("fullName") or owner.get("name")
    owner_team_name = t.get("ownerTeam")  # string

    org_id = None
    org_name = None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
        org_id = org.get("id")
        org_name = org.get("businessName") or org.get("fullName") or org.get("name")

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
        "owner_team_name": owner_team_name,
        "organization_id": org_id,
        "organization_name": org_name
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_name,organization_id,organization_name)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(owner_id)s,%(owner_name)s,%(owner_team_name)s,%(organization_id)s,%(organization_name)s)
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
 organization_name    = excluded.organization_name
"""

def upsert_rows(rows: List[Dict[str, Any]]) -> List[int]:
    if not rows:
        return []
    with conn() as c, c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    return [r["ticket_id"] for r in rows]

# ------------------------------------------------------------------------------
# Audit helpers
# ------------------------------------------------------------------------------
def get_last_detail_run_at() -> datetime:
    with conn() as c, c.cursor() as cur:
        cur.execute("select coalesce(max(last_detail_run_at), timestamp 'epoch') from visualizacao_resolvidos.sync_control where name='default'")
        val = cur.fetchone()[0]
    if val is None or val == datetime(1970,1,1):
        return datetime.now(timezone.utc) - timedelta(days=7)
    if val.tzinfo is None:
        return val.replace(tzinfo=timezone.utc)
    return val

def set_last_detail_run_now() -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
            values('default',now(),now())
            on conflict (name) do update set last_update=now(), last_detail_run_at=now()
        """)

def get_audit_ids_latest_run() -> Tuple[int, List[int]]:
    with conn() as c, c.cursor() as cur:
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0] or 0
        if run_id == 0:
            return 0, []
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where run_id=%s and table_name='tickets_resolvidos'
             order by ticket_id
        """, (run_id,))
        ids = [r[0] for r in cur.fetchall()]
    return run_id, ids

def cleanup_audit_if_persisted(run_id: int, processed_ids: List[int]) -> None:
    if run_id <= 0 or not processed_ids:
        return
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where ticket_id = any(%s)
        """, (processed_ids,))
        present = [r[0] for r in cur.fetchall()]
        if not present:
            return
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing
             where run_id=%s and table_name='tickets_resolvidos' and ticket_id = any(%s)
        """, (run_id, present))

# ------------------------------------------------------------------------------
# Execução principal
# ------------------------------------------------------------------------------
def process_pages(pages_iter: Iterable[List[Dict[str, Any]]], label: str) -> int:
    total = 0
    for page in pages_iter:
        rows = [map_row(t) for t in page if t.get("id")]
        if not rows:
            continue
        upsert_rows(rows)
        total += len(rows)
    logging.info("%s: upsert %d", label, total)
    return total

def main():
    ensure_schema()

    # 1) Reprocessa pendências do audit (último run)
    run_id, audit_ids = get_audit_ids_latest_run()
    if audit_ids:
        logging.info("Audit pendentes (run_id=%s): %d", run_id, len(audit_ids))
        processed: List[int] = []
        for page in fetch_by_ids(audit_ids):
            rows = [map_row(t) for t in page if t.get("id")]
            if not rows:
                continue
            ids = upsert_rows(rows)
            processed.extend(ids)
        logging.info("Audit: upsert %d", len(processed))
        cleanup_audit_if_persisted(run_id, processed)

    # 2) Incremental por lastUpdate
    since = get_last_detail_run_at()
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")
    logging.info("Incremental desde: %s", since_iso)
    process_pages(fetch_pages_since(since_iso), "Incremental")

    set_last_detail_run_now()

if __name__ == "__main__":
    main()
