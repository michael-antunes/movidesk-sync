#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza AÇÕES dos tickets resolvidos/fechados/cancelados (até 10 por execução).
Lê IDs de visualizacao_resolvidos.tickets_resolvidos e faz upsert em
visualizacao_resolvidos.resolvidos_acoes.

Secrets necessários:
- MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN)
- NEON_DSN
"""

import os
import json
import time
import typing as t
import requests
import psycopg2
import psycopg2.extras as pgx

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

BATCH_SIZE  = int(os.getenv("MOVIDESK_BATCH", "10"))
PG_PAGESIZE = int(os.getenv("MOVIDESK_PG_PAGESIZE", "200"))

API_URL = "https://api.movidesk.com/public/v1/tickets"

SCHEMA = "visualizacao_resolvidos"
TABLE_TICKETS = f"{SCHEMA}.tickets_resolvidos"
TABLE_ACOES   = f"{SCHEMA}.resolvidos_acoes"


def get_conn():
    if not NEON_DSN or not API_TOKEN:
        raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")
    return psycopg2.connect(NEON_DSN)


def select_next_ticket_ids(conn, limit: int) -> t.List[int]:
    """
    Pega tickets que nunca foram gravados em resolvidos_acoes
    ou que tiveram 'last_activity' posterior ao updated_at das ações.
    """
    sql = f"""
    with cand as (
        select
            tr.ticket_id,
            greatest(
                coalesce(tr.last_resolved_at,  'epoch'::timestamptz),
                coalesce(tr.last_closed_at,    'epoch'::timestamptz),
                coalesce(tr.last_cancelled_at, 'epoch'::timestamptz),
                coalesce(tr.last_update,       'epoch'::timestamptz)
            ) as last_activity
        from {TABLE_TICKETS} tr
    )
    select c.ticket_id
    from cand c
    left join {TABLE_ACOES} ra on ra.ticket_id = c.ticket_id
    where ra.updated_at is null
       or ra.updated_at < c.last_activity
    order by c.last_activity asc
    limit %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def movidesk_request(params: dict, max_retries: int = 3, backoff_s: float = 1.5):
    for i in range(max_retries):
        r = requests.get(API_URL, params=params, timeout=60)
        try:
            r.raise_for_status()
            return r.json()
        except requests.HTTPError:
            if r.status_code in (429, 500, 502, 503, 504) and i < max_retries - 1:
                time.sleep(backoff_s * (i + 1))
                continue
            raise


def fetch_actions_for_ids(ids: t.List[int]) -> t.Dict[int, t.List[dict]]:
    """
    Busca ações dos tickets informados. Inclui descriptionHtml, attachments,
    e timeAppointments conforme a KB do Movidesk.
    """
    if not ids:
        return {}

    filter_expr = " or ".join([f"id eq {i}" for i in ids])

    expand_inner = (
        "actions("
        "$select=id,isPublic,origin,createdDate,description,descriptionHtml,attachments,timeAppointments;"
        "$expand=attachments,timeAppointments"
        ")"
    )

    params = {
        "token": API_TOKEN,
        "$select": "id,lastUpdate,actions",
        "$expand": expand_inner,
        "$filter": filter_expr,
        "$top": 100
    }

    data = movidesk_request(params) or []
    out: t.Dict[int, t.List[dict]] = {}
    for item in data:
        tid = int(item.get("id"))
        acts = item.get("actions") or []
        out[tid] = acts
    return out


def upsert_actions(conn, rows: t.List[t.Tuple[int, list]]):
    if not rows:
        return
    sql = f"""
    insert into {TABLE_ACOES} (ticket_id, acoes, updated_at)
    values (%s, %s::jsonb, now())
    on conflict (ticket_id) do update
        set acoes = excluded.acoes,
            updated_at = now();
    """
    payload = [(tid, json.dumps(acts, ensure_ascii=False)) for tid, acts in rows]
    with conn.cursor() as cur:
        pgx.execute_batch(cur, sql, payload, page_size=PG_PAGESIZE)
    conn.commit()


def main():
    conn = get_conn()
    try:
        ids = select_next_ticket_ids(conn, BATCH_SIZE)
        if not ids:
            print("Sem candidatos para sincronizar.")
            return

        print(f"Processando {len(ids)} ticket(s): {ids}")
        actions_map = fetch_actions_for_ids(ids)
        rows = [(tid, actions_map.get(tid, [])) for tid in ids]  # grava [] quando não há ações
        upsert_actions(conn, rows)
        print("Concluído com sucesso.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
