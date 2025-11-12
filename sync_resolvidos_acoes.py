#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

# Configurações
BATCH_SIZE        = int(os.getenv("ACTIONS_BATCH_SIZE", "10"))   # 10 tickets por lote
MAX_BATCHES       = int(os.getenv("ACTIONS_MAX_BATCHES", "1"))   # processa 1 lote e sai
THROTTLE_SEC      = float(os.getenv("ACTIONS_THROTTLE_SEC", "0.4"))
PAGE_SIZE_ACTIONS = int(os.getenv("ACTIONS_PAGE_SIZE", "100"))
BACKFILL_DAYS     = int(os.getenv("ACTIONS_BACKFILL_DAYS", "30"))
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "90"))       # segundos
HTTP_RETRIES      = int(os.getenv("HTTP_RETRIES", "2"))          # tentativas extras em 429/5xx

BASE_URL = "https://api.movidesk.com/public/v1"

# ------------------------ HTTP util ------------------------
_session = None
def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"Accept":"application/json"})
    return _session

def http_get(url, params):
    if not API_TOKEN:
        raise RuntimeError("Defina MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN)")
    q = dict(params or {})
    q["token"] = API_TOKEN

    sess = get_session()
    tries = 1 + HTTP_RETRIES
    for attempt in range(tries):
        try:
            r = sess.get(url, params=q, timeout=HTTP_TIMEOUT)
            # backoff simples para 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt + 1 < tries:
                    sleep = min(5 * (attempt + 1), 15)
                    time.sleep(sleep)
                    continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt + 1 >= tries:
                raise
            time.sleep(2 * (attempt + 1))
    return []  # nunca chega aqui

def qident(name: str) -> str:
    return '"' + name.replace('"','""') + '"'

# ------------------------ DB util ------------------------
def get_conn():
    if not NEON_DSN:
        raise RuntimeError("Defina NEON_DSN")
    return psycopg2.connect(NEON_DSN)

def list_columns(conn, schema: str, table: str):
    sql = """
      select column_name
      from information_schema.columns
      where table_schema=%s and table_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return {row[0] for row in cur.fetchall()}

def pick_column(available: set, candidates):
    for c in candidates:
        if c in available:
            return c
    return None

# ------------------------ API ------------------------
def fetch_actions_for_ticket(ticket_id: int):
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
        page = data if isinstance(data, list) else (data or [])
        all_actions.extend(page)
        if len(page) < PAGE_SIZE_ACTIONS:
            break
        skip += PAGE_SIZE_ACTIONS
        time.sleep(THROTTLE_SEC)
    return all_actions

# ------------------------ DB - controle ------------------------
def get_last_run(conn) -> datetime:
    with conn.cursor() as cur:
        cur.execute("""
            select last_update
            from visualizacao_resolvidos.sync_control
            where name='acoes'
            order by last_update desc
            limit 1
        """)
        row = cur.fetchone()
    if row and row[0]:
        return row[0]
    return datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)

def heartbeat(conn):
    with conn.cursor() as cur:
        cur.execute("""
            insert into visualizacao_resolvidos.sync_control(name, last_update)
            values ('acoes', now())
            on conflict (name) do update set last_update = excluded.last_update
        """)
    conn.commit()

# ------------------------ Seleção dos próximos tickets ------------------------
def select_next_ticket_ids(conn, limit: int):
    cols = list_columns(conn, "visualizacao_resolvidos", "tickets_resolvidos")

    pk = pick_column(cols, ["id", "ticket_id"])
    if not pk:
        raise RuntimeError("Não encontrei a PK em visualizacao_resolvidos.tickets_resolvidos (id ou ticket_id).")

    resolved_col = pick_column(cols, ["resolvedIn","resolvedin","resolved_in"])
    closed_col   = pick_column(cols, ["closedIn","closedin","closed_in"])
    canceled_col = pick_column(cols, ["canceledIn","canceledin","canceled_in"])
    lastupd_col  = pick_column(cols, ["lastUpdate","lastupdate","last_update"])

    # GREATEST pode não usar índice; mas fica simples e consistente.
    parts = []
    if resolved_col: parts.append(qident(resolved_col))
    if closed_col:   parts.append(qident(closed_col))
    if canceled_col: parts.append(qident(canceled_col))
    if parts:
        resolved_expr = "GREATEST(" + ",".join(parts) + ")"
    elif lastupd_col:
        resolved_expr = qident(lastupd_col)
    else:
        resolved_expr = "now()"

    last_run = get_last_run(conn)

    # JOINs explícitos: evita erro de referência ao alias
    sql = f"""
        with ctl as (
          select %s::timestamptz as last_run
        ),
        src as (
          select {qident(pk)} as ticket_id,
                 {resolved_expr} as resolved_at
          from visualizacao_resolvidos.tickets_resolvidos
        )
        select s.ticket_id
        from src s
        cross join ctl c
        left join visualizacao_resolvidos.resolvidos_acoes ra
               on ra.ticket_id = s.ticket_id
        where
          ra.ticket_id is null
          or (s.resolved_at is not null and s.resolved_at >= c.last_run - interval '1 day')
        order by s.resolved_at desc nulls last, s.ticket_id desc
        limit %s
    """

    with conn.cursor() as cur:
        cur.execute(sql, (last_run, limit))
        return [r[0] for r in cur.fetchall()]

def upsert_actions(conn, rows):
    sql = """
        insert into visualizacao_resolvidos.resolvidos_acoes
            (ticket_id, acoes, updated_at)
        values (%s, %s::jsonb, now())
        on conflict (ticket_id)
        do update set
            acoes = excluded.acoes,
            updated_at = now()
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()

# ------------------------ MAIN ------------------------
def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

    conn = get_conn()
    try:
        for batch_no in range(1, MAX_BATCHES + 1):
            ids = select_next_ticket_ids(conn, BATCH_SIZE)
            print(f"[acoes] batch {batch_no}: {len(ids)} tickets para processar")
            if not ids:
                break

            payload = []
            for tid in ids:
                try:
                    actions = fetch_actions_for_ticket(tid)
                    payload.append((tid, json.dumps(actions, ensure_ascii=False)))
                    print(f"[acoes] ticket {tid}: {len(actions)} ações")
                except Exception as e:
                    # guarda vazio para não travar o pipeline; você pode logar se preferir
                    print(f"[acoes] ticket {tid}: erro {e}; salvando []")
                    payload.append((tid, "[]"))
                finally:
                    time.sleep(THROTTLE_SEC)

            if payload:
                upsert_actions(conn, payload)

            # marca batida de coração 1x por lote
            heartbeat(conn)

            # processa somente até MAX_BATCHES (padrão 1) e sai
            if batch_no >= MAX_BATCHES:
                break
    finally:
        conn.close()

if __name__ == "__main__":
    main()
