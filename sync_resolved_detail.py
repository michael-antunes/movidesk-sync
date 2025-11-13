# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Iterable, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# Config/log
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s  %(message)s"
)

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

BASE_URL   = "https://api.movidesk.com/public/v1/tickets"
THROTTLE   = float(os.getenv("THROTTLE_SEC", "0.25"))

# Para não estourar "MaxNodeCount 100" do OData:
# cada "id eq N" + "or" conta como nó; 25 é seguro.
ID_GROUP_SIZE = int(os.getenv("AUDIT_ID_GROUP", "25"))

# Paging para busca incremental por lastUpdate quando não houver audit
UP_SINCE_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
PAGE_TOP      = int(os.getenv("PAGE_TOP", "100"))

# ----------------------------
# Conexão / helpers
# ----------------------------
def conn():
    return psycopg2.connect(NEON_DSN)

def req(url: str, params: Dict[str, Any], retries: int = 4) -> Any:
    """
    GET com repetição em 429/5xx e mensagens de erro mais claras em 400.
    """
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.25 * (i + 1))
            continue
        # Erros "400 Bad Request" do Movidesk são comuns se $select/$expand estiverem errados
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} - url: {r.url} - body: {r.text}",
            response=r
        )
    r.raise_for_status()

def ensure_schema():
    """
    Garante schema/tabelas mínimas sem DROPs (o drop foi externalizado).
    Também garante as colunas owner_* e organização, para o upsert não falhar.
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")

        # tickets_resolvidos (sem excluir nada aqui)
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

        # adiciona colunas (se não existirem)
        def addcol(col: str, typ: str):
            cur.execute(f"""
            do $$
            begin
              if not exists (
                select 1 from information_schema.columns
                where table_schema='visualizacao_resolvidos'
                  and table_name='tickets_resolvidos'
                  and column_name=%s
              ) then
                execute 'alter table visualizacao_resolvidos.tickets_resolvidos add column {col} {typ}';
              end if;
            end$$
            """, (col,))

        addcol("owner_id", "text")
        addcol("owner_name", "text")
        addcol("owner_team_id", "text")
        addcol("owner_team_name", "text")
        addcol("organization_id", "text")
        addcol("organization_name", "text")
        addcol("last_update", "timestamptz")

        # sync_control para guardar último run
        cur.execute("""
        create table if not exists visualizacao_resolvidos.sync_control(
          name text primary key,
          last_update timestamptz default now(),
          last_detail_run_at timestamptz
        )
        """)

        # tabelas de auditoria mínimas (caso ainda não existam)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          window_start timestamptz,
          window_end timestamptz,
          total_api integer,
          missing_total integer,
          run_at timestamptz,
          window_from timestamptz,
          window_to timestamptz,
          total_local integer,
          notes text
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint not null,
          table_name text not null,
          ticket_id integer not null
        )
        """)
        cur.execute("""
        create table if not exists visualizacao_resolvidos.audit_ticket_watch(
          ticket_id integer primary key,
          times integer not null default 0,
          last_reason text,
          last_ts timestamptz default now()
        )
        """)

def get_last_detail_run() -> datetime:
    with conn() as c, c.cursor() as cur:
        cur.execute("""
          select coalesce(max(last_detail_run_at), timestamp 'epoch')
          from visualizacao_resolvidos.sync_control
          where name='detail'
        """)
        ts = cur.fetchone()[0]
    if ts is None or ts == datetime(1970,1,1):
        return datetime.now(timezone.utc) - timedelta(days=UP_SINCE_DAYS)
    return ts

def set_last_detail_run():
    with conn() as c, c.cursor() as cur:
        cur.execute("""
        insert into visualizacao_resolvidos.sync_control(name,last_update,last_detail_run_at)
        values ('detail', now(), now())
        on conflict (name) do update set last_update = now(), last_detail_run_at = now()
        """)

def chunked(seq: Iterable[int], n: int) -> Iterable[List[int]]:
    buf: List[int] = []
    for x in seq:
        buf.append(int(x))
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

# ----------------------------
# Movidesk fetchers
# ----------------------------
TICKET_SELECT = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency",
    "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel"
])
# Mantemos expand simples para não quebrar: owner e org do 1º client
TICKET_EXPAND = "owner,clients($expand=organization)"

def build_ids_filter(ids: List[int]) -> str:
    # id eq 123 or id eq 456 ...
    return " or ".join([f"id eq {i}" for i in ids])

def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    params = {
        "token": API_TOKEN,
        "$select": TICKET_SELECT,
        "$expand": TICKET_EXPAND,
        "$filter": build_ids_filter(ids),
        "$top": 100
    }
    data = req(BASE_URL, params) or []
    time.sleep(THROTTLE)
    return data

def fetch_by_ids(all_ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    for group in chunked(all_ids, ID_GROUP_SIZE):
        page = fetch_group_by_ids(group)
        if page:
            yield page

def fetch_incremental(since_iso: str) -> Iterable[List[Dict[str, Any]]]:
    """
    Fallback: caso não haja audit pendente, busca por lastUpdate >= since_iso.
    """
    skip = 0
    total = 0
    while True:
        params = {
            "token": API_TOKEN,
            "$select": TICKET_SELECT,
            "$expand": TICKET_EXPAND,
            "$filter": "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled') and lastUpdate ge %s" % since_iso,
            "$orderby": "lastUpdate asc",
            "$top": PAGE_TOP,
            "$skip": skip
        }
        page = req(BASE_URL, params) or []
        if not page:
            break
        yield page
        got = len(page)
        total += got
        skip += got
        if got < PAGE_TOP:
            break
        time.sleep(THROTTLE)

# ----------------------------
# Map / Upsert
# ----------------------------
def to_utc(dt_str: Any):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(str(dt_str).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = (t.get("owner") or {})
    owner_id = owner.get("id")
    # businessName ou fullName (varia por tenant)
    owner_name = owner.get("businessName") or owner.get("fullName") or owner.get("name")

    # Organização (pelo 1º client)
    org_id = None
    org_name = None
    clients = t.get("clients") or []
    if clients:
        org = (clients[0] or {}).get("organization") or {}
        org_id = org.get("id")
        # businessName ou fullName
        org_name = org.get("businessName") or org.get("fullName") or org.get("name")

    # Team do responsável:
    # Como o endpoint de tickets costuma não expor ownerTeam de forma estável,
    # deixamos como None (a trigger cuidará de mandar ao audit se isso for obrigatório).
    owner_team_id = None
    owner_team_name = None

    return {
        "ticket_id": int(t.get("id")),
        "status": t.get("status"),
        "last_resolved_at": to_utc(t.get("resolvedIn")),
        "last_closed_at":   to_utc(t.get("closedIn")),
        "last_cancelled_at":to_utc(t.get("canceledIn")),
        "last_update":      to_utc(t.get("lastUpdate")),
        "origin":  t.get("origin"),
        "category":t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
        "owner_id": owner_id,
        "owner_name": owner_name,
        "owner_team_id": owner_team_id,
        "owner_team_name": owner_team_name,
        "organization_id": org_id,
        "organization_name": org_name
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_id,owner_team_name,organization_id,organization_name)
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
 owner_team_id=excluded.owner_team_id,
 owner_team_name=excluded.owner_team_name,
 organization_id=excluded.organization_id,
 organization_name=excluded.organization_name
"""

def upsert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn() as c, c.cursor() as cur:
        execute_values(cur, UPSERT_SQL, [(
            r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"], r["last_cancelled_at"],
            r["last_update"], r["origin"], r["category"], r["urgency"],
            r["service_first_level"], r["service_second_level"], r["service_third_level"],
            r["owner_id"], r["owner_name"], r["owner_team_id"], r["owner_team_name"],
            r["organization_id"], r["organization_name"]
        ) for r in rows])

# ----------------------------
# Audit helpers
# ----------------------------
def get_latest_audit_missing_ids() -> List[int]:
    """
    Busca o último run_id e retorna os ticket_ids da audit para table_name='tickets_resolvidos'.
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0]
        if not run_id:
            return []
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where run_id=%s and table_name='tickets_resolvidos'
        """, (run_id,))
        ids = [r[0] for r in cur.fetchall() or []]
    return ids

def delete_audit_for_present(present_ids: List[int]) -> int:
    """
    Remove ONLY os tickets da audit_recent_missing que já estão efetivamente na tabela final.
    Isso evita limpar IDs que a trigger barrou (e devem continuar pendentes).
    """
    if not present_ids:
        return 0
    with conn() as c, c.cursor() as cur:
        # confere o último run novamente (idempotente)
        cur.execute("select coalesce(max(id),0) from visualizacao_resolvidos.audit_recent_run")
        run_id = cur.fetchone()[0]
        if not run_id:
            return 0
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing m
             using visualizacao_resolvidos.tickets_resolvidos t
             where m.run_id=%s
               and m.table_name='tickets_resolvidos'
               and m.ticket_id = any(%s)
               and t.ticket_id = m.ticket_id
        """, (run_id, list(present_ids)))
        deleted = cur.rowcount
    return deleted

def select_present_ids(ids: List[int]) -> List[int]:
    if not ids:
        return []
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where ticket_id = any(%s)
        """, (list(ids),))
        return [r[0] for r in cur.fetchall() or []]

# ----------------------------
# Main
# ----------------------------
def main():
    ensure_schema()

    # 1) Processa primeiro o que estiver no audit_recent_missing (tickets_resolvidos)
    audit_ids = get_latest_audit_missing_ids()
    logging.info("Audit pendentes: %d", len(audit_ids))

    inserted_now: List[int] = []
    if audit_ids:
        total = 0
        for page in fetch_by_ids(audit_ids):
            rows = [map_row(t) for t in page]
            upsert_rows(rows)
            # verifica quem realmente entrou (trigger pode ter barrado linhas com owner/team nulos)
            ids_page = [r["ticket_id"] for r in rows]
            present = select_present_ids(ids_page)
            inserted_now.extend(present)
            total += len(rows)
            logging.info("Audit page: fetched=%d upserted(ok)=%d", len(rows), len(present))
        # remove da audit apenas os que confirmadamente entraram
        removed = delete_audit_for_present(inserted_now)
        logging.info("Audit cleanup: removed_from_audit=%d", removed)

    # 2) Busca incremental por lastUpdate como fallback (quando não há audit, ou para manter atualizado)
    #    Mantemos pequeno para não atropelar a janela; você pode ajustar conforme necessidade.
    since = get_last_detail_run()
    since_iso = since.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    logging.info("Incremental since (UTC): %s", since_iso)

    inc_total = 0
    for page in fetch_incremental(since_iso):
        rows = [map_row(t) for t in page]
        upsert_rows(rows)
        # não limpamos audit aqui; essa etapa é para manter em dia, o audit é limpo no bloco acima
        inc_total += len(rows)
        logging.info("Incremental page: fetched=%d", len(rows))

    set_last_detail_run()
    logging.info("Fim. audit_upsert_ok=%d incremental_fetched=%d", len(inserted_now), inc_total)


if __name__ == "__main__":
    main()
