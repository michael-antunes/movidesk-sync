# -*- coding: utf-8 -*-
import os
import time
from typing import Iterable, List, Dict, Any
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone

# ========= Config =========
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

TOP_PAGES = int(os.getenv("PAGES_UPSERT", "7"))            # paginacao por lastUpdate
THROTTLE  = float(os.getenv("THROTTLE_SEC", "0.5"))
ID_BATCH  = int(os.getenv("AUDIT_ID_BATCH", "50"))         # tamanho máximo inicial do lote

# ========= Conexão =========
def db():
    return psycopg2.connect(NEON_DSN)

# ========= Controle de execução =========
SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values('detail', now(), now())
on conflict (name) do update set last_update = now(), last_detail_run_at = now()
"""

GET_LASTRUN = """
select coalesce(max(last_detail_run_at), timestamp 'epoch' at time zone 'UTC')
from visualizacao_resolvidos.sync_control
where name = 'detail'
"""

# ========= HTTP helper =========
def req(url: str, params: Dict[str, Any], retries: int = 4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        # Retentativas para throttling/transientes
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * (i + 1))
            continue
        # Erro definitivo
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}",
            response=r
        )
    r.raise_for_status()

# ========= Utils =========
def to_utc(dt_str: str | None):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def chunked(seq: Iterable[int], n: int) -> Iterable[List[int]]:
    buf: List[int] = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

# ========= Mapeamento =========
def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    org_id = None
    org_name = None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
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
        "owner_name": owner.get("businessName") or owner.get("fullName") or owner.get("name"),
        "organization_id": org_id,
        "organization_name": org_name,
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_name,organization_id,organization_name)
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(owner_name)s,%(organization_id)s,%(organization_name)s)
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
 owner_name           = excluded.owner_name,
 organization_id      = excluded.organization_id,
 organization_name    = excluded.organization_name
"""

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    with db() as c, c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    return len(rows)

# ========= Incremental por lastUpdate =========
def fetch_pages(since_iso: str):
    base = "https://api.movidesk.com/public/v1/tickets"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency",
        "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"
    filtro = ("(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') "
              f"and lastUpdate ge {since_iso}")
    skip = 0
    total = 0
    hard_top = TOP_PAGES * 100

    while True:
        page = req(base, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$expand": expand,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
            "$top": min(100, hard_top - total),
            "$skip": skip
        }) or []
        if not page:
            break
        yield page
        got = len(page)
        total += got
        skip += got
        if total >= hard_top or got < 100:
            break
        time.sleep(THROTTLE)

# ========= Fetch por IDs (robusto contra URL grande) =========
def _fetch_group_by_ids(group: List[int]) -> List[Dict[str, Any]]:
    """
    Tenta buscar um grupo de IDs numa única chamada. Se a API responder 400 (URL / OData excessivo),
    divide o grupo em 2 e tenta recursivamente até 1-por-1.
    """
    base = "https://api.movidesk.com/public/v1/tickets"
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
        "origin","category","urgency",
        "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
    ])
    expand = "owner,clients($expand=organization)"

    if not group:
        return []

    try:
        filter_expr = " or ".join([f"id eq {int(i)}" for i in group])
        page = req(base, {
            "token": API_TOKEN,
            "$select": select_fields,
            "$expand": expand,
            "$filter": filter_expr,
            "$top": 100
        }) or []
        time.sleep(THROTTLE)
        return page
    except requests.HTTPError as e:
        # Se for 400, a estratégia é dividir e tentar em metades
        if e.response is not None and e.response.status_code == 400 and len(group) > 1:
            mid = len(group) // 2
            left = _fetch_group_by_ids(group[:mid])
            right = _fetch_group_by_ids(group[mid:])
            return (left or []) + (right or [])
        # Outros erros ou já em 1:1 -> propaga
        raise

def fetch_by_ids(ids: List[int]):
    """
    Generator que busca os IDs vindos do audit em lotes (máx inicial = ID_BATCH),
    com fallback automático em subdivisão quando a API retornar 400.
    """
    for group in chunked(ids, ID_BATCH):
        data = _fetch_group_by_ids(group)
        yield data

def pick_audit_ticket_ids() -> List[int]:
    with db() as c, c.cursor() as cur:
        cur.execute("""
            select ticket_id
            from visualizacao_resolvidos.audit_recent_missing
            where table_name = 'tickets_resolvidos'
            order by ticket_id
        """)
        return [r[0] for r in cur.fetchall()]

def drop_from_audit(ids: List[int]):
    if not ids:
        return
    with db() as c, c.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            delete from visualizacao_resolvidos.audit_recent_missing a
            where a.table_name = 'tickets_resolvidos'
              and (a.ticket_id) in %s
        """, [(i,) for i in ids])

# ========= Main =========
def main():
    # 1) Incremental por lastUpdate
    with db() as c, c.cursor() as cur:
        cur.execute(GET_LASTRUN)
        since_dt = cur.fetchone()[0]

    if since_dt == datetime(1970,1,1,tzinfo=timezone.utc):
        since_dt = datetime.now(timezone.utc) - timedelta(days=7)
    since_iso = since_dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows_inc: List[Dict[str, Any]] = []
    for page in fetch_pages(since_iso):
        for t in page:
            rows_inc.append(map_row(t))
    if rows_inc:
        upsert_rows(rows_inc)

    with db() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)

    # 2) Reprocessar pendências do audit (tickets_resolvidos)
    audit_ids = pick_audit_ticket_ids()
    if audit_ids:
        rows_audit: List[Dict[str, Any]] = []
        for page in fetch_by_ids(audit_ids):
            for t in page:
                rows_audit.append(map_row(t))
        if rows_audit:
            upsert_rows(rows_audit)
            done_ids = [r["ticket_id"] for r in rows_audit if r.get("ticket_id")]
            drop_from_audit(done_ids)

if __name__ == "__main__":
    main()
