#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, math
import requests
import psycopg2, psycopg2.extras
from datetime import datetime, timezone

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

# parâmetros ajustáveis por env
BATCH_SIZE          = int(os.getenv("ACTIONS_BATCH_SIZE", "10"))   # 10 tickets por vez
THROTTLE_SEC        = float(os.getenv("ACTIONS_THROTTLE_SEC", "0.4"))
PAGE_SIZE_ACTIONS   = 100  # paginação da API de ações

BASE_URL = "https://api.movidesk.com/public/v1"

def http_get(url, params):
    # evita None no token
    if not API_TOKEN:
        raise RuntimeError("Defina MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN)")
    q = dict(params or {})
    q["token"] = API_TOKEN
    r = requests.get(url, params=q, timeout=90)
    r.raise_for_status()
    return r.json()

def get_conn():
    if not NEON_DSN:
        raise RuntimeError("Defina NEON_DSN")
    return psycopg2.connect(NEON_DSN)

def select_next_ticket_ids(conn, limit=BATCH_SIZE):
    """
    Seleciona até 'limit' tickets que precisam atualizar ações:
      - não existem na resolvidos_acoes, OU
      - tickets_resolvidos.last_update > resolvidos_acoes.updated_at
    """
    sql = """
        with tr as (
            select id, last_update
            from visualizacao_resolvidos.tickets_resolvidos
        )
        select tr.id
        from tr
        left join visualizacao_resolvidos.resolvidos_acoes ra
               on ra.ticket_id = tr.id
        where ra.ticket_id is null
           or tr.last_update > coalesce(ra.updated_at, '1970-01-01'::timestamptz)
        order by tr.last_update desc nulls last, tr.id desc
        limit %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]

def fetch_actions_for_ticket(ticket_id):
    """
    Busca ações pela rota /ticketActions paginando, incluindo:
      - description (HTML)
      - isPublic, createdDate, origin
      - attachments (id, fileName, link)
      - timeAppointments (id, startDate, endDate, workTime)
    """
    url = f"{BASE_URL}/ticketActions"
    all_actions = []
    skip = 0
    while True:
        params = {
            "$filter": f"ticket/id eq {ticket_id} and isDeleted eq false",
            "$select": "id,isPublic,description,createdDate,origin",
            "$expand": "attachments($select=id,fileName,link),timeAppointments($select=id,startDate,endDate,workTime)",
            "$top": PAGE_SIZE_ACTIONS,
            "$skip": skip
        }
        data = http_get(url, params)
        if not isinstance(data, list):
            # proteção caso a API venha como objeto
            page = data or []
        else:
            page = data

        all_actions.extend(page)
        if len(page) < PAGE_SIZE_ACTIONS:
            break
        skip += PAGE_SIZE_ACTIONS
        time.sleep(THROTTLE_SEC)  # bem leve

    return all_actions

def upsert_actions(conn, rows):
    """
    rows: lista de tuplas (ticket_id, acoes_json_text)
    """
    sql = """
        insert into visualizacao_resolvidos.resolvidos_acoes
            (ticket_id, acoes, updated_at)
        values (%s, %s::jsonb, now())
        on conflict (ticket_id)
        do update set
            acoes = excluded.acoes,
            updated_at = now();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()

def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

    conn = get_conn()
    try:
        while True:
            ids = select_next_ticket_ids(conn, BATCH_SIZE)
            if not ids:
                # nada para fazer agora
                break

            out_rows = []
            for tid in ids:
                try:
                    actions = fetch_actions_for_ticket(tid)
                    # guardamos exatamente como veio (lista de objetos)
                    acoes_json = json.dumps(actions, ensure_ascii=False)
                    out_rows.append((tid, acoes_json))
                except requests.HTTPError as e:
                    # HTTP 404 / 400 etc — armazena vazio mas não quebra lote
                    out_rows.append((tid, "[]"))
                except Exception:
                    out_rows.append((tid, "[]"))
                finally:
                    time.sleep(THROTTLE_SEC)

            if out_rows:
                upsert_actions(conn, out_rows)

            # loop continua até esvaziar a fila (ids vazios)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
