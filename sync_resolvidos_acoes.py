#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

# Parâmetros
BATCH_SIZE        = int(os.getenv("ACTIONS_BATCH_SIZE", "10"))     # tickets por vez
THROTTLE_SEC      = float(os.getenv("ACTIONS_THROTTLE_SEC", "0.4")) # intervalo entre chamadas
PAGE_SIZE_ACTIONS = int(os.getenv("ACTIONS_PAGE_SIZE", "100"))
BACKFILL_DAYS     = int(os.getenv("ACTIONS_BACKFILL_DAYS", "30"))   # fallback se não houver last_run

BASE_URL = "https://api.movidesk.com/public/v1"

# ---------- util ----------
def http_get(url, params):
    if not API_TOKEN:
        raise RuntimeError("Defina MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN)")
    q = dict(params or {})
    q["token"] = API_TOKEN
    r = requests.get(url, params=q, timeout=90)
    r.raise_for_status()
    return r.json()

def qident(name: str) -> str:
    # quote de identificador postgres
    return '"' + name.replace('"','""') + '"'

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

def pick_column(available: set, candidates: list[str]) -> str | None:
    # devolve o primeiro candidato existente (case-sensitive)
    for c in candidates:
        if c in available:
            return c
    return None

# ---------- API Movidesk ----------
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

# ---------- DB ----------
def get_last_run(conn) -> datetime:
    """
    Usa sync_control(name='acoes').last_update.
    Se não existir, volta BACKFILL_DAYS atrás.
    """
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
    # fallback
    return datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)

def heartbeat(conn):
    with conn.cursor() as cur:
        cur.execute("""
            insert into visualizacao_resolvidos.sync_control(name, last_update)
            values ('acoes', now())
            on conflict (name) do update set last_update = excluded.last_update
        """)
    conn.commit()

def select_next_ticket_ids(conn, limit: int):
    """
    Seleciona até 'limit' tickets com base:
     - data de resolução >= última execução do 'acoes' (com margem de 1 dia)
     - OU ausentes na resolvidos_acoes
     - ordena por resolução desc
    Esquema agnóstico (id ou ticket_id; camelCase / snake_case para datas).
    """
    cols = list_columns(conn, "visualizacao_resolvidos", "tickets_resolvidos")

    pk = pick_column(cols, ["id", "ticket_id"])
    if not pk:
        raise RuntimeError("Não encontrei a PK em visualizacao_resolvidos.tickets_resolvidos (id ou ticket_id).")

    # datas possíveis
    resolved_candidates  = ["resolvedIn", "resolvedin", "resolved_in"]
    closed_candidates    = ["closedIn", "closedin", "closed_in"]
    canceled_candidates  = ["canceledIn", "canceledin", "canceled_in"]
    lastupd_candidates   = ["lastUpdate", "lastupdate", "last_update"]

    resolved_col = pick_column(cols, resolved_candidates)
    closed_col   = pick_column(cols, closed_candidates)
    canceled_col = pick_column(cols, canceled_candidates)
    lastupd_col  = pick_column(cols, lastupd_candidates)

    # expressão de data de resolução
    date_expr_parts = []
    if resolved_col: date_expr_parts.append(qident(resolved_col))
    if closed_col:   date_expr_parts.append(qident(closed_col))
    if canceled_col: date_expr_parts.append(qident(canceled_col))

    if date_expr_parts:
        resolved_expr = "GREATEST(" + ",".join(date_expr_parts) + ")"
    elif lastupd_col:
        resolved_expr = qident(lastupd_col)  # fallback
    else:
        # último fallback: usa 'now()' para não quebrar
        resolved_expr = "now()"

    last_run = get_last_run(conn)

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
        from src s, ctl c
        left join visualizacao_resolvidos.resolvidos_acoes ra
               on ra.ticket_id = s.ticket_id
        where
          -- não existe em ações
          ra.ticket_id is null
          -- ou foi resolvido após última execução (com margem de 1 dia)
          or (s.resolved_at is not null and s.resolved_at >= c.last_run - interval '1 day')
        order by s.resolved_at desc nulls last, s.ticket_id desc
        limit %s
    """

    with conn.cursor() as cur:
        cur.execute(sql, (last_run, limit))
        return [r[0] for r in cur.fetchall()]

def upsert_actions(conn, rows):
    """
    rows: [(ticket_id, acoes_json_text), ...]
    """
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

# ---------- MAIN ----------
def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

    conn = get_conn()
    try:
        while True:
            ids = select_next_ticket_ids(conn, BATCH_SIZE)
            if not ids:
                break

            payload = []
            for tid in ids:
                try:
                    actions = fetch_actions_for_ticket(tid)
                    payload.append((tid, json.dumps(actions, ensure_ascii=False)))
                except Exception:
                    # Falha na API? Não trava o lote; grava vazio para não loopar
                    payload.append((tid, "[]"))
                finally:
                    time.sleep(THROTTLE_SEC)

            if payload:
                upsert_actions(conn, payload)

            heartbeat(conn)  # marca última execução bem-sucedida
    finally:
        conn.close()

if __name__ == "__main__":
    main()
