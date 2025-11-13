# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Iterable

import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s  %(message)s"
)

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"

# ------------ parâmetros de execução ------------
THROTTLE_SEC        = float(os.getenv("THROTTLE_SEC", "0.35"))
PAGES_UPSERT        = int(os.getenv("PAGES_UPSERT", "7"))   # só para o fallback por janela temporal
FILTER_GROUP_SIZE   = int(os.getenv("FILTER_GROUP_SIZE", "25"))  # ids por chamada OData (<=25 fica bem abaixo do node-limit 100)
MAX_AUDIT_IDS       = int(os.getenv("MAX_AUDIT_IDS", "600"))     # máximo de ids vindos do audit por execução
FALLBACK_DAYS       = int(os.getenv("FALLBACK_DAYS", "7"))       # janela temporal caso o audit esteja vazio

# ------------ seleção / expand para a API ------------
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
    "subject",          # assunto
    "ownerTeam"         # nome da equipe (string simples)
])

# IMPORTANTE: sem fullName; apenas businessName (algumas contas expõem 'name', tratamos no fallback do map)
EXPAND = "owner($select=id,businessName),clients($expand=organization($select=id,businessName))"

# ------------ SQL ------------
UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 subject, owner_id, owner_name, owner_team_name, organization_id, organization_name)
values (%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
        %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
        %(subject)s, %(owner_id)s, %(owner_name)s, %(owner_team_name)s, %(organization_id)s, %(organization_name)s)
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
 subject              = excluded.subject,
 owner_id             = excluded.owner_id,
 owner_name           = excluded.owner_name,
 owner_team_name      = excluded.owner_team_name,
 organization_id      = excluded.organization_id,
 organization_name    = excluded.organization_name
"""

# coluna adicional_nome (código 29077) — guardamos num UPDATE para não poluir o upsert acima
UPSERT_ADICIONAL_NOME = """
update visualizacao_resolvidos.tickets_resolvidos
   set adicional_nome = %s
 where ticket_id = %s
"""

SET_LASTRUN = """
insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
values ('default', now(), now())
on conflict (name) do update set last_update = now(), last_detail_run_at = now()
"""

GET_LASTRUN = """
select coalesce(max(last_detail_run_at), timestamp 'epoch')
  from visualizacao_resolvidos.sync_control
 where name = 'default'
"""

# ------------ util ------------
def conn():
    return psycopg2.connect(NEON_DSN)

def z(dt_str: Any):
    """Converte string ISO da API para UTC (timestamptz) ou None."""
    if not dt_str:
        return None
    try:
        s = str(dt_str).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def req(url: str, params: dict):
    # monta com token sem escapar os símbolos do OData (para $filter grande)
    from urllib.parse import urlencode
    q = {"token": API_TOKEN, **params}
    full = f"{url}?{urlencode(q, safe='(),$= :')}"
    r = requests.get(full, timeout=60)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return []
    # mensagens claras pra log
    raise requests.HTTPError(
        f"{r.status_code} {r.reason} - url: {full} - body: {r.text}",
        response=r
    )

# ------------ schema ------------
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
          subject text,
          owner_id text,
          owner_name text,
          owner_team_name text,
          organization_id text,
          organization_name text,
          adicional_nome text
        )
        """)
        # adiciona colunas que porventura não existam ainda (sem dropar nada)
        for col, ddl in [
            ("subject",              "alter table visualizacao_resolvidos.tickets_resolvidos add column subject text"),
            ("owner_id",             "alter table visualizacao_resolvidos.tickets_resolvidos add column owner_id text"),
            ("owner_name",           "alter table visualizacao_resolvidos.tickets_resolvidos add column owner_name text"),
            ("owner_team_name",      "alter table visualizacao_resolvidos.tickets_resolvidos add column owner_team_name text"),
            ("organization_id",      "alter table visualizacao_resolvidos.tickets_resolvidos add column organization_id text"),
            ("organization_name",    "alter table visualizacao_resolvidos.tickets_resolvidos add column organization_name text"),
            ("adicional_nome",       "alter table visualizacao_resolvidos.tickets_resolvidos add column adicional_nome text"),
        ]:
            cur.execute("""
                do $$
                begin
                  if not exists(
                    select 1 from information_schema.columns
                     where table_schema='visualizacao_resolvidos'
                       and table_name='tickets_resolvidos'
                       and column_name=%s
                  ) then execute %s; end if;
                end$$
            """, (col, ddl))

# ------------ mapeamento dos campos ------------
def extract_owner_name(owner: Dict[str, Any]) -> str:
    # API vária entre 'businessName' e 'name'
    return owner.get("businessName") or owner.get("name")

def extract_org_name(org: Dict[str, Any]) -> str:
    return org.get("businessName") or org.get("name")

def extract_adicional_nome(custom_fields: Any) -> Any:
    """
    Campo adicional 'nome' (código 29077).
    Considera diferentes formatos possíveis vindos da API.
    """
    if not custom_fields:
        return None

    for cf in custom_fields:
        cid = cf.get("id")
        ccustom = cf.get("customFieldId")
        label = (cf.get("label") or cf.get("name") or "").strip().lower()
        # identifica pelo código
        if str(ccustom or cid) == "29077" or "29077" in str(cid):
            val = cf.get("value")
            if isinstance(val, dict):
                return val.get("name") or val.get("value")
            if isinstance(val, list):
                return ", ".join(map(str, val))
            return val
        # fallback por label/descrição
        if "nome" in label:
            val = cf.get("value")
            if isinstance(val, dict):
                return val.get("name") or val.get("value")
            if isinstance(val, list):
                return ", ".join(map(str, val))
            return val

    return None

def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    owner_id   = owner.get("id")
    owner_name = extract_owner_name(owner)

    org_id = None
    org_nm = None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
        org_id = org.get("id")
        org_nm = extract_org_name(org)

    # adicional nome (código 29077) — retorna string/None
    adicional_nome = extract_adicional_nome(t.get("customFields"))

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
        "subject": t.get("subject"),
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_team_name": t.get("ownerTeam"),
        "organization_id": org_id,
        "organization_name": org_nm,
        "adicional_nome": adicional_nome,
    }

# ------------ busca por IDs (audit) ------------
def chunk(iterable: Iterable[int], size: int) -> Iterable[List[int]]:
    bucket = []
    for x in iterable:
        bucket.append(x)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket

def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    """
    Busca 1 página por grupo de ids (ids já vem limitados em FILTER_GROUP_SIZE).
    """
    filt = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND,
        "$filter": filt,
        "$top": 100  # não interfere pois filtro é por ids
    }
    data = req(BASE_URL, params) or []
    time.sleep(THROTTLE_SEC)
    return data

def fetch_by_ids(all_ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    """
    Itera em grupos pequenos para não estourar o node-limit do OData.
    """
    for group in chunk(all_ids, FILTER_GROUP_SIZE):
        yield fetch_group_by_ids(group)

# ------------ fallback por janela temporal ------------
def fetch_by_window(since_iso: str) -> Iterable[List[Dict[str, Any]]]:
    """
    Varredura por lastUpdate (fallback quando não há itens no audit).
    """
    got = 0
    skip = 0
    top_total = PAGES_UPSERT * 100  # respeita a mesma lógica antiga (100 por página)
    filtro = f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge {since_iso}"
    while True:
        params = {
            "$select": SELECT_FIELDS,
            "$expand": EXPAND,
            "$filter": filtro,
            "$orderby": "lastUpdate asc",
            "$top": min(100, top_total - got),
            "$skip": skip
        }
        data = req(BASE_URL, params) or []
        if not data:
            break
        yield data
        got += len(data)
        skip += len(data)
        if got >= top_total or len(data) < 100:
            break
        time.sleep(THROTTLE_SEC)

# ------------ leitura do audit ------------
def load_audit_ids() -> List[int]:
    with conn() as c, c.cursor() as cur:
        # pega os ids pendentes mais recentes (independente de run)
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
             order by run_id desc, ticket_id desc
             limit %s
        """, (MAX_AUDIT_IDS,))
        rows = cur.fetchall() or []
    ids = [int(r[0]) for r in rows]
    logging.info("Audit pendentes: %d", len(ids))
    return ids

def remove_from_audit(success_ids: List[int]):
    if not success_ids:
        return
    with conn() as c, c.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            delete from visualizacao_resolvidos.audit_recent_missing a
             where a.table_name = 'tickets_resolvidos'
               and a.ticket_id = any(%s)
        """, (success_ids,))
        # não é necessário COMMIT explícito (context manager faz)

# ------------ persistência ------------
def upsert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn() as c, c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
        # adicional_nome num update à parte (só quando houver)
        ad_vals = [(r["adicional_nome"], r["ticket_id"])
                   for r in rows if r.get("adicional_nome") is not None]
        if ad_vals:
            psycopg2.extras.execute_batch(cur, UPSERT_ADICIONAL_NOME, ad_vals, page_size=200)

# ------------ main ------------
def main():
    ensure_schema()

    # 1) tenta pelo audit
    audit_ids = load_audit_ids()

    success_ids: List[int] = []
    total_upserts = 0

    if audit_ids:
        for page in fetch_by_ids(audit_ids):
            mapped = [map_row(t) for t in page]
            upsert_rows(mapped)
            total_upserts += len(mapped)
            success_ids.extend([m["ticket_id"] for m in mapped])

        remove_from_audit(success_ids)
        logging.info("Upserts (audit): %d | removidos do audit: %d", total_upserts, len(success_ids))

    # 2) fallback por janela (se não havia audit)
    if not audit_ids:
        with conn() as c, c.cursor() as cur:
            cur.execute(GET_LASTRUN)
            since = cur.fetchone()[0]
        if since == datetime(1970,1,1,tzinfo=timezone.utc):
            since = datetime.now(timezone.utc) - timedelta(days=FALLBACK_DAYS)
        since_iso = since.replace(microsecond=0).isoformat().replace("+00:00","Z")

        total_upserts = 0
        for page in fetch_by_window(since_iso):
            mapped = [map_row(t) for t in page]
            upsert_rows(mapped)
            total_upserts += len(mapped)
        logging.info("Upserts (fallback janela): %d", total_upserts)

    # 3) marca o last run sempre que rodar
    with conn() as c, c.cursor() as cur:
        cur.execute(SET_LASTRUN)

if __name__ == "__main__":
    main()
