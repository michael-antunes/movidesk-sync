# sync_resolved_detail.py
# -*- coding: utf-8 -*-

import os
import time
import logging
from typing import Dict, Any, List, Iterable
from urllib.parse import urlencode

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"
TOKEN    = os.environ["MOVIDESK_TOKEN"]
DSN      = os.environ["NEON_DSN"]

THROTTLE      = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
AUDIT_LIMIT   = int(os.getenv("DETAIL_AUDIT_LIMIT", "1000"))  # quantos IDs pegar do audit_recent_missing
ODATA_MAX_OR  = int(os.getenv("DETAIL_GROUP_SIZE", "10"))     # máx. IDs por requisição para não estourar MaxNodeCount

# ---- helpers HTTP / OData ----------------------------------------------------

def req(url: str, params: dict) -> Any:
    """GET simples com formatação do OData preservando operadores."""
    q = {"token": TOKEN, **params}
    # manter parênteses, vírgulas, $ e espaços da OData
    qs = urlencode(q, safe="(),$= '")
    r = requests.get(f"{url}?{qs}", timeout=60)
    if r.status_code != 200:
        body = (r.text or "").strip()[:600]
        raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {r.url} - body: {body}", response=r)
    return r.json()


def chunked(seq: Iterable[int], size: int) -> Iterable[List[int]]:
    buf: List[int] = []
    for x in seq:
        buf.append(int(x))
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def build_ids_filter(ids: List[int]) -> str:
    # 'id eq 1 or id eq 2 ...' (sem IN na OData pública)
    return " or ".join(f"id eq {i}" for i in ids)

# ---- schema ------------------------------------------------------------------

def ensure_schema():
    """Garante que as colunas novas existam sem derrubar nada."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:

        def has_col(name: str) -> bool:
            cur.execute("""
                select 1
                  from information_schema.columns
                 where table_schema='visualizacao_resolvidos'
                   and table_name='tickets_resolvidos'
                   and column_name=%s
                limit 1
            """, (name,))
            return cur.fetchone() is not None

        def addcol(name: str, typ: str):
            if not has_col(name):
                cur.execute(f"alter table visualizacao_resolvidos.tickets_resolvidos add column {name} {typ}")

        # Novas/confirmadas
        addcol("owner_id",           "text")
        addcol("owner_name",         "text")
        addcol("owner_team_name",    "text")
        addcol("subject",            "text")
        addcol("adicional_29077_nome", "text")

        # Campos padrão (só garante que existem, caso o banco tenha sido criado do zero)
        addcol("ticket_id",          "integer")
        addcol("status",             "text")
        addcol("last_resolved_at",   "timestamptz")
        addcol("last_closed_at",     "timestamptz")
        addcol("origin",             "text")
        addcol("category",           "text")
        addcol("urgency",            "text")
        addcol("service_first_level",  "text")
        addcol("service_second_level", "text")
        addcol("service_third_level",  "text")
        addcol("organization_id",    "text")
        addcol("organization_name",  "text")

        # índice de PK se não existir (evita duplicidade)
        cur.execute("""
            do $$
            begin
              if not exists (
                select 1 from pg_indexes
                 where schemaname='visualizacao_resolvidos'
                   and indexname='tickets_resolvidos_pkey'
              ) then
                begin
                  alter table visualizacao_resolvidos.tickets_resolvidos
                    add constraint tickets_resolvidos_pkey primary key (ticket_id);
                exception when others then null;
                end;
              end if;
            end$$;
        """)

        conn.commit()

# ---- fonte de IDs: audit_recent_missing --------------------------------------

def fetch_audit_ids(limit: int) -> List[int]:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            select distinct ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
             order by ticket_id
             limit %s
        """, (limit,))
        rows = [r[0] for r in cur.fetchall()]
    return rows

# ---- mapeamento Movidesk -> tabela ------------------------------------------

def first_org_from_clients(t: Dict[str, Any]) -> Dict[str, Any]:
    for cli in (t.get("clients") or []):
        org = cli.get("organization") or {}
        # retorna o primeiro que tiver ao menos id ou businessName
        if ("id" in org) or ("businessName" in org):
            return {"organization_id": org.get("id"), "organization_name": org.get("businessName")}
    return {"organization_id": None, "organization_name": None}


def extract_adicional_29077(t: Dict[str, Any]) -> str | None:
    # customFieldValues: lista de dicts (ver doc)
    for cf in (t.get("customFieldValues") or []):
        try:
            if int(cf.get("customFieldId") or 0) == 29077:
                # normalmente texto simples em 'value'; se vier em 'items', tenta o nome
                val = cf.get("value")
                if val:
                    return str(val)
                items = cf.get("items") or []
                if items:
                    # tenta pegar o 'customFieldItem' do primeiro item
                    return items[0].get("customFieldItem")
                return None
        except Exception:
            continue
    return None


def map_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    org   = first_org_from_clients(t)

    row = {
        "ticket_id":            int(t.get("id")),
        "status":               t.get("status"),
        "last_resolved_at":     t.get("resolvedIn"),
        "last_closed_at":       t.get("closedIn"),
        "origin":               t.get("origin"),
        "category":             t.get("category"),
        "urgency":              t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
        "subject":              t.get("subject"),
        "owner_id":             owner.get("id"),
        "owner_name":           owner.get("businessName"),
        "owner_team_name":      t.get("ownerTeam"),
        "organization_id":      org["organization_id"],
        "organization_name":    org["organization_name"],
        "adicional_29077_nome": extract_adicional_29077(t),
    }
    return row

# ---- persistência ------------------------------------------------------------

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        execute_values(cur, """
            insert into visualizacao_resolvidos.tickets_resolvidos
                (ticket_id, status, last_resolved_at, last_closed_at,
                 origin, category, urgency,
                 service_first_level, service_second_level, service_third_level,
                 subject, owner_id, owner_name, owner_team_name,
                 organization_id, organization_name,
                 adicional_29077_nome)
            values %s
            on conflict (ticket_id) do update set
                status               = excluded.status,
                last_resolved_at     = excluded.last_resolved_at,
                last_closed_at       = excluded.last_closed_at,
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
                organization_name    = excluded.organization_name,
                adicional_29077_nome = excluded.adicional_29077_nome
        """, [(
            r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"],
            r["origin"], r["category"], r["urgency"],
            r["service_first_level"], r["service_second_level"], r["service_third_level"],
            r["subject"], r["owner_id"], r["owner_name"], r["owner_team_name"],
            r["organization_id"], r["organization_name"],
            r["adicional_29077_nome"],
        ) for r in rows])
        conn.commit()
    return len(rows)

# ---- coleta MOVIDESK por IDs -------------------------------------------------

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
    "ownerTeam",
])

# Atenção:
#  - owner: só 'id' e 'businessName' existem (nada de fullName)
#  - clients -> organization: 'id' e 'businessName'
#  - customFieldValues vem via $expand (não use 'customFields' no $select)
EXPAND = "owner($select=id,businessName),clients($expand=organization($select=id,businessName)),customFieldValues"

def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND,
        "$filter": build_ids_filter(ids),
        "$top": 100
    }
    data = req(BASE_URL, params) or []
    return data

def fetch_by_ids(all_ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    for group in chunked(all_ids, ODATA_MAX_OR):
        # cada grupo gera UMA chamada (evita estourar MaxNodeCount=100)
        try:
            data = fetch_group_by_ids(group)
            yield data
        except requests.HTTPError as e:
            # loga e continua com grupos menores, se necessário
            logging.error("Erro ao buscar grupo %s: %s", group, e)
            # Tenta dividir o grupo ao meio ao encontrar o erro de nó (fallback)
            if len(group) > 1 and "node count limit" in (str(e).lower()):
                mid = len(group)//2
                for sub in (group[:mid], group[mid:]):
                    try:
                        data = fetch_group_by_ids(sub)
                        yield data
                    except Exception as ee:
                        logging.error("Falhou subgrupo %s: %s", sub, ee)
            else:
                # não re-tenta
                continue
        finally:
            time.sleep(THROTTLE)

# ---- main --------------------------------------------------------------------

def main():
    ensure_schema()

    audit_ids = fetch_audit_ids(AUDIT_LIMIT)
    logging.info("Audit pendentes: %d", len(audit_ids))
    if not audit_ids:
        logging.info("Nenhum ticket pendente no audit_recent_missing para 'tickets_resolvidos'. Encerrando.")
        return

    out_rows: List[Dict[str, Any]] = []
    for page in fetch_by_ids(audit_ids):
        for t in page or []:
            try:
                out_rows.append(map_ticket_row(t))
            except Exception as e:
                logging.warning("Falha mapeando ticket: %s | erro: %s", t.get("id"), e)

    total = upsert_rows(out_rows)
    logging.info("Upsert concluído: %d registros em tickets_resolvidos.", total)
    logging.info("Obs.: remoção em caso de owner/team NULL é responsabilidade da trigger já criada.")

if __name__ == "__main__":
    main()
