# -*- coding: utf-8 -*-
"""
Sync resolved/closed/cancelled detail (Movidesk -> visualizacao_resolvidos.tickets_resolvidos)

Fluxo:
- Lê IDs em visualizacao_resolvidos.audit_recent_missing (table_name='tickets_resolvidos')
- Busca detalhes no /tickets (OData) em grupos pequenos (evita MaxNodeCount)
- Upsert em visualizacao_resolvidos.tickets_resolvidos
- Campos novos: subject, adicional_nome (código 29077), adicional_29077_nome (espelho)
- NÃO usa ownerTeamId; somente ownerTeam (texto)
- Se a API rejeitar customFieldValues no $expand, refaz a chamada sem isso.

ENV:
- MOVIDESK_TOKEN
- NEON_DSN
- MOVIDESK_THROTTLE  (default 0.25)
- AUDIT_LIMIT_IDS    (default 600)
- AUDIT_GROUP_SIZE   (default 20)
"""

import os
import time
import logging
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlencode

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE_URL = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN = os.environ["NEON_DSN"]

THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
AUDIT_LIMIT_IDS = int(os.getenv("AUDIT_LIMIT_IDS", "600"))
AUDIT_GROUP_SIZE = int(os.getenv("AUDIT_GROUP_SIZE", "20"))

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
    "subject",      # assunto
    "ownerTeam"     # nome da equipe
])

# Usar customFieldValues sem $select (o DTO não tem 'id', e alguns ambientes variam).
EXPAND_WITH_CF = (
    "owner($select=id,businessName),"
    "clients($expand=organization($select=id,businessName)),"
    "customFieldValues"
)
EXPAND_NO_CF = (
    "owner($select=id,businessName),"
    "clients($expand=organization($select=id,businessName))"
)

# Código/identificadores possíveis do adicional "nome"
CUSTOM_FIELD_ID_NOME = {"29077", 29077, "adicional_29077_nome"}


# --------------------------------------------------------------------------- #
# Schema helpers
# --------------------------------------------------------------------------- #
def ensure_schema(conn) -> None:
    def addcol(col: str, typ: str) -> None:
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
            logging.info("added column %s %s", col, typ)

    addcol("owner_team_name", "text")
    addcol("subject", "text")
    addcol("adicional_nome", "text")
    addcol("adicional_29077_nome", "text")


# --------------------------------------------------------------------------- #
# HTTP (Movidesk)
# --------------------------------------------------------------------------- #
def req(url: str, params: Dict[str, Any], allow_cf: bool = True) -> List[Dict[str, Any]]:
    base_params = {"token": TOKEN}
    qs = urlencode({**base_params, **params}, safe="(),$= ")
    full = f"{url}?{qs}"
    r = requests.get(full, timeout=60)
    if r.status_code == 200:
        return r.json() or []

    body = (r.text or "")
    # Se deu 400 relacionado a custom fields, refaz sem customFieldValues
    if r.status_code == 400 and allow_cf and (
        "customFieldValues" in body
        or "CustomFieldValue" in body
        or "CustomField" in body
    ):
        new_params = dict(params)
        new_params["$expand"] = EXPAND_NO_CF
        qs2 = urlencode({**base_params, **new_params}, safe="(),$= ")
        full2 = f"{url}?{qs2}"
        r2 = requests.get(full2, timeout=60)
        if r2.status_code == 200:
            return r2.json() or []
        raise requests.HTTPError(f"{r2.status_code} {r2.reason} - url: {full2} - body: {r2.text}", response=r2)

    raise requests.HTTPError(f"{r.status_code} {r.reason} - url: {full} - body: {body}", response=r)


def chunked(it: Iterable[int], size: int) -> Iterable[List[int]]:
    buf: List[int] = []
    for x in it:
        buf.append(int(x))
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# --------------------------------------------------------------------------- #
# Audit IDs
# --------------------------------------------------------------------------- #
def load_audit_ids(conn) -> List[int]:
    with conn.cursor() as cur:
        cur.execute("""
            select distinct ticket_id
              from visualizacao_resolvidos.audit_recent_missing
             where table_name = 'tickets_resolvidos'
             order by ticket_id desc
             limit %s
        """, (AUDIT_LIMIT_IDS,))
        rows = [r[0] for r in cur.fetchall() or []]
    logging.info("Audit pendentes: %d", len(rows))
    return rows


# --------------------------------------------------------------------------- #
# Mapping
# --------------------------------------------------------------------------- #
def _pick_org_from_clients(t: Dict[str, Any]) -> Tuple[Any, Any]:
    for c in (t.get("clients") or []):
        org = c.get("organization") or {}
        if org.get("id") or org.get("businessName"):
            return org.get("id"), org.get("businessName")
    return None, None


def _pick_custom_29077(t: Dict[str, Any]) -> str | None:
    # Estruturas possíveis: customFieldValues (oficial), mas mantemos fallback.
    for arr in [t.get("customFieldValues"), t.get("customFields"), t.get("customFieldItems")]:
        if not arr:
            continue
        for item in arr:
            iid = item.get("customFieldId") or item.get("id") or item.get("name")
            if iid in CUSTOM_FIELD_ID_NOME or str(iid) in CUSTOM_FIELD_ID_NOME:
                val = item.get("value")
                if isinstance(val, (str, int, float)):
                    return str(val)
                if isinstance(val, dict) and "name" in val:
                    return str(val["name"])
                if isinstance(val, list) and val:
                    for v in val:
                        if isinstance(v, (str, int, float)):
                            return str(v)
                        if isinstance(v, dict) and "name" in v:
                            return str(v["name"])
    return None


def map_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    org_id, org_name = _pick_org_from_clients(t)
    add_nome = _pick_custom_29077(t)

    return {
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
        "owner_id": owner.get("id"),
        "owner_name": owner.get("businessName"),
        "owner_team_name": t.get("ownerTeam"),
        "organization_id": org_id,
        "organization_name": org_name,
        "subject": t.get("subject"),
        "adicional_nome": add_nome,
        "adicional_29077_nome": add_nome,
    }


# --------------------------------------------------------------------------- #
# Upsert & marks
# --------------------------------------------------------------------------- #
def upsert_tickets(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, """
            insert into visualizacao_resolvidos.tickets_resolvidos
            (
                ticket_id,
                status,
                last_resolved_at,
                last_closed_at,
                last_cancelled_at,
                last_update,
                origin,
                category,
                urgency,
                service_first_level,
                service_second_level,
                service_third_level,
                owner_id,
                owner_name,
                owner_team_name,
                organization_id,
                organization_name,
                subject,
                adicional_nome,
                adicional_29077_nome
            )
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
                adicional_nome       = excluded.adicional_nome,
                adicional_29077_nome = excluded.adicional_29077_nome
        """, [(
            r["ticket_id"],
            r["status"],
            r["last_resolved_at"],
            r["last_closed_at"],
            r["last_cancelled_at"],
            r["last_update"],
            r["origin"],
            r["category"],
            r["urgency"],
            r["service_first_level"],
            r["service_second_level"],
            r["service_third_level"],
            r["owner_id"],
            r["owner_name"],
            r["owner_team_name"],
            r["organization_id"],
            r["organization_name"],
            r.get("subject"),
            r.get("adicional_nome"),
            r.get("adicional_29077_nome"),
        ) for r in rows])
    conn.commit()
    return len(rows)


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


def delete_from_audit_only_if_ok(conn, ids: List[int]) -> None:
    """
    Remove do audit_recent_missing APENAS tickets que ficaram gravados com
    owner_name e owner_team_name preenchidos (respeitando a trigger).
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            delete from visualizacao_resolvidos.audit_recent_missing a
            using visualizacao_resolvidos.tickets_resolvidos t
            where a.table_name = 'tickets_resolvidos'
              and a.ticket_id  = t.ticket_id
              and t.owner_name is not null
              and t.owner_team_name is not null
              and a.ticket_id = any(%s)
        """, (ids,))
    conn.commit()


# --------------------------------------------------------------------------- #
# Fetch by IDs (grupos pequenos)
# --------------------------------------------------------------------------- #
def _fetch_ids_once(ids: List[int]) -> List[Dict[str, Any]]:
    f = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND_WITH_CF,
        "$filter": f,
        "$top": 100
    }
    data = req(BASE_URL, params, allow_cf=True) or []
    time.sleep(THROTTLE)
    return data


def fetch_group_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    return _fetch_ids_once(ids)


def fetch_by_ids(all_ids: List[int]) -> Iterable[List[Dict[str, Any]]]:
    for group in chunked(all_ids, AUDIT_GROUP_SIZE):
        yield fetch_group_by_ids(group)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    with psycopg2.connect(DSN) as conn:
        ensure_schema(conn)

        audit_ids = load_audit_ids(conn)
        if not audit_ids:
            logging.info("Nada a processar.")
            return

        total_upserts = 0
        processed_ids: List[int] = []

        for page in fetch_by_ids(audit_ids):
            if not page:
                continue
            rows = [map_ticket_row(t) for t in page if t and t.get("id")]
            if not rows:
                continue

            upsert_tickets(conn, rows)
            ids = [r["ticket_id"] for r in rows]
            mark_sync(conn, ids)
            processed_ids.extend(ids)
            total_upserts += len(rows)

        if processed_ids:
            delete_from_audit_only_if_ok(conn, processed_ids)

        logging.info("tickets_resolvidos: upsert %d (processados=%d, grupo=%d)",
                     total_upserts, len(processed_ids), AUDIT_GROUP_SIZE)


if __name__ == "__main__":
    main()
