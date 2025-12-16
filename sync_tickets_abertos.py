import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

RUN_DDL = os.getenv("RUN_DDL", "0") == "1"
PG_SYNC_COMMIT_OFF = os.getenv("PG_SYNC_COMMIT_OFF", "1") == "1"

CSAT_CUSTOM_FIELD_ID = 137641

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tickets_abertos_index")

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def req(url, params=None, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("Throttle HTTP %s, aguardando %ss…", r.status_code, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def norm_ts(x):
    if not x:
        return None
    s = str(x).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s


def set_fast_commit(cur):
    if PG_SYNC_COMMIT_OFF:
        cur.execute("set local synchronous_commit=off")


def fetch_open_tickets():
    logger.info("Iniciando fetch de tickets abertos em /tickets…")

    url = f"{API_BASE}/tickets"
    skip = 0
    fil = "(baseStatus eq 'New' or baseStatus eq 'InAttendance' or baseStatus eq 'Stopped')"

    select_fields = [
        "id",
        "subject",
        "type",
        "status",
        "baseStatus",
        "ownerTeam",
        "serviceFirstLevel",
        "origin",
        "createdDate",
        "lastUpdate",
        "reopenedIn",
    ]
    sel = ",".join(select_fields)
    expand = "owner,clients($expand=organization),customFieldValues"

    items = []

    while True:
        params = {
            "token": TOKEN,
            "$select": sel,
            "$expand": expand,
            "$filter": fil,
            "$orderby": "lastUpdate asc",
            "$top": PAGE_SIZE,
            "$skip": skip,
        }

        page = req(url, params=params) or []
        if not page:
            break

        items.extend(page)
        logger.info("Página com %s tickets (acumulado %s)", len(page), len(items))

        if len(page) < PAGE_SIZE:
            break

        skip += len(page)
        time.sleep(THROTTLE)

    logger.info("Total de tickets abertos retornados: %s", len(items))
    return items


def extract_csat(custom_fields):
    if not custom_fields or not isinstance(custom_fields, list):
        return None

    for cf in custom_fields:
        try:
            if int(cf.get("customFieldId")) != CSAT_CUSTOM_FIELD_ID:
                continue
        except Exception:
            continue

        if cf.get("value") is not None:
            return str(cf.get("value"))
        items = cf.get("items") or []
        if items and isinstance(items, list):
            first = items[0] or {}
            if first.get("customFieldItem") is not None:
                return str(first.get("customFieldItem"))
        return None

    return None


def map_row(t):
    clients = t.get("clients") or []
    org_id = None
    org_name = None
    if isinstance(clients, list) and clients:
        for c in clients:
            org = (c or {}).get("organization") or {}
            if org.get("id"):
                org_id = org.get("id")
                org_name = org.get("businessName")
                break

    owner = t.get("owner") or {}
    custom_fields = t.get("customFieldValues") or []

    ticket_id = iint(t.get("id"))
    created_val = norm_ts(t.get("createdDate"))
    last_update_val = norm_ts(t.get("lastUpdate"))

    reopened_val = norm_ts(t.get("reopenedIn"))
    reaberturas = 1 if reopened_val else 0

    return {
        "ticket_id": ticket_id,
        "subject": t.get("subject"),
        "type": iint(t.get("type")),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "owner_team": t.get("ownerTeam"),
        "service_first_level": t.get("serviceFirstLevel"),
        "created_date": created_val,
        "last_update": last_update_val,
        "origin": iint(t.get("origin")),
        "contagem": 1,
        "responsavel": owner.get("businessName"),
        "agent_id": iint(owner.get("id")),
        "empresa_id": org_id,
        "empresa_nome": org_name,
        "empresa_cod_ref_adicional": None,
        "adicional_137641_avaliado_csat": extract_csat(custom_fields),
        "reaberturas": reaberturas,
    }


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_atual")
        cur.execute(
            """
            create table if not exists visualizacao_atual.tickets_abertos(
              ticket_id       bigint primary key,
              raw             jsonb,
              updated_at      timestamptz default now(),
              raw_last_update timestamptz,
              last_update     timestamptz,
              base_status     text
            )
            """
        )
        cur.execute(
            """
            alter table visualizacao_atual.tickets_abertos
              add column if not exists subject                       varchar(350),
              add column if not exists type                          smallint,
              add column if not exists status                        varchar(128),
              add column if not exists owner_team                    varchar(128),
              add column if not exists service_first_level           varchar(102),
              add column if not exists created_date                  timestamptz,
              add column if not exists contagem                      integer default 1,
              add column if not exists responsavel                   varchar(255),
              add column if not exists empresa_cod_ref_adicional     varchar(64),
              add column if not exists agent_id                      bigint,
              add column if not exists empresa_id                    text,
              add column if not exists empresa_nome                  text,
              add column if not exists adicional_137641_avaliado_csat text,
              add column if not exists origin                        smallint,
              add column if not exists reaberturas                   integer default 0
            """
        )
    conn.commit()


def upsert_tickets(conn, rows):
    if not rows:
        return 0

    now_utc = datetime.now(timezone.utc)

    values = []
    for r in rows:
        if r.get("ticket_id") is None:
            continue
        values.append(
            (
                r.get("ticket_id"),
                r.get("subject"),
                r.get("type"),
                r.get("status"),
                r.get("base_status"),
                r.get("owner_team"),
                r.get("service_first_level"),
                r.get("created_date"),
                r.get("last_update"),
                r.get("origin"),
                r.get("contagem", 1),
                r.get("responsavel"),
                r.get("empresa_cod_ref_adicional"),
                r.get("agent_id"),
                r.get("empresa_id"),
                r.get("empresa_nome"),
                r.get("adicional_137641_avaliado_csat"),
                r.get("reaberturas", 0),
                now_utc,
            )
        )

    if not values:
        return 0

    sql = """
        insert into visualizacao_atual.tickets_abertos (
            ticket_id,
            subject,
            type,
            status,
            base_status,
            owner_team,
            service_first_level,
            created_date,
            last_update,
            origin,
            contagem,
            responsavel,
            empresa_cod_ref_adicional,
            agent_id,
            empresa_id,
            empresa_nome,
            adicional_137641_avaliado_csat,
            reaberturas,
            updated_at
        ) values %s
        on conflict (ticket_id) do update set
            subject                       = excluded.subject,
            type                          = excluded.type,
            status                        = excluded.status,
            base_status                   = excluded.base_status,
            owner_team                    = excluded.owner_team,
            service_first_level           = excluded.service_first_level,
            created_date                  = excluded.created_date,
            last_update                   = excluded.last_update,
            origin                        = excluded.origin,
            contagem                      = excluded.contagem,
            responsavel                   = excluded.responsavel,
            empresa_cod_ref_adicional     = excluded.empresa_cod_ref_adicional,
            agent_id                      = excluded.agent_id,
            empresa_id                    = excluded.empresa_id,
            empresa_nome                  = excluded.empresa_nome,
            adicional_137641_avaliado_csat = excluded.adicional_137641_avaliado_csat,
            reaberturas                   = excluded.reaberturas,
            updated_at                    = excluded.updated_at
        where visualizacao_atual.tickets_abertos.last_update is distinct from excluded.last_update
    """

    with conn.cursor() as cur:
        set_fast_commit(cur)
        execute_values(cur, sql, values, page_size=200)

    conn.commit()
    return len(values)


def cleanup_not_open(conn, open_ids):
    if not open_ids:
        with conn.cursor() as cur:
            set_fast_commit(cur)
            cur.execute("delete from visualizacao_atual.tickets_abertos")
            removed = cur.rowcount
        conn.commit()
        if removed:
            logger.info("Removendo %s tickets (nenhum aberto na API).", removed)
        return removed

    unique_ids = sorted({i for i in open_ids if i is not None})
    if not unique_ids:
        return 0

    with conn.cursor() as cur:
        set_fast_commit(cur)
        cur.execute(
            "create temporary table tmp_open_ids(ticket_id bigint primary key) on commit drop"
        )
        execute_values(
            cur,
            "insert into tmp_open_ids(ticket_id) values %s",
            [(i,) for i in unique_ids],
            page_size=1000,
        )

        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             where not exists (
               select 1 from tmp_open_ids t
                where t.ticket_id = ta.ticket_id
             )
            """
        )
        removed = cur.rowcount

    conn.commit()
    if removed:
        logger.info("Removendo %s tickets que não estão mais abertos.", removed)
    return removed


def cleanup_merged(conn):
    with conn.cursor() as cur:
        set_fast_commit(cur)
        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             using visualizacao_resolvidos.tickets_mesclados tm
             where ta.ticket_id = tm.ticket_id
            """
        )
        removed = cur.rowcount

    conn.commit()
    if removed:
        logger.info("Removendo %s tickets marcados como mesclados.", removed)
    return removed


def update_empresa_cod_ref(conn):
    with conn.cursor() as cur:
        set_fast_commit(cur)
        cur.execute(
            """
            update visualizacao_atual.tickets_abertos ta
               set empresa_cod_ref_adicional = e.codereferenceadditional
              from visualizacao_empresa.empresas e
             where ta.empresa_id = e.id
               and (ta.empresa_cod_ref_adicional is distinct from e.codereferenceadditional)
            """
        )
        affected = cur.rowcount

    conn.commit()
    logger.info(
        "Atualização de empresa_cod_ref_adicional concluída (linhas afetadas: %s).",
        affected,
    )
    return affected


def main():
    logger.info("Iniciando sync rápido de tickets abertos (index enriquecido).")

    items = fetch_open_tickets()
    rows = [map_row(t) for t in items if isinstance(t, dict) and t.get("id") is not None]
    open_ids = [r.get("ticket_id") for r in rows if r.get("ticket_id") is not None]

    with psycopg2.connect(DSN) as conn:
        if RUN_DDL:
            ensure_table(conn)

        inserted = upsert_tickets(conn, rows)
        logger.info("Inseridos/atualizados %s tickets na tabela tickets_abertos.", inserted)

        removed_closed = cleanup_not_open(conn, open_ids)
        removed_merged = cleanup_merged(conn)
        update_empresa_cod_ref(conn)

    logger.info(
        "Sync de tickets abertos (index) finalizado com sucesso. "
        "Inseridos/atualizados=%s, removidos(fechados/apagados)=%s, removidos(mesclados)=%s.",
        inserted,
        removed_closed,
        removed_merged,
    )


if __name__ == "__main__":
    main()
