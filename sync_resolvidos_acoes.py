#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza AÇÕES dos tickets resolvidos/fechados/cancelados.
- Pega até 10 ticket_ids por execução a partir de visualizacao_resolvidos.tickets_resolvidos
- Busca as ações do ticket (com attachments e timeAppointments) pela API
- Grava em visualizacao_resolvidos.resolvidos_acoes (jsonb) e atualiza updated_at
- Respeita rate limit (10 req/min) com throttle configurável

Requisitos (env):
  - MOVIDESK_TOKEN  (ou MOVIDESK_API_TOKEN)
  - NEON_DSN        (postgres DSN)
Opcionais (env):
  - MOVIDESK_BATCH_SIZE   (padrão 10)
  - MOVIDESK_THROTTLE     (segundos entre chamadas – padrão 7)
"""

import os
import sys
import json
import time
import math
import datetime as dt
from typing import List, Tuple, Any, Dict

import psycopg2
import psycopg2.extras
import requests


API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
BATCH_SIZE = int(os.getenv("MOVIDESK_BATCH_SIZE", "10"))
THROTTLE = int(os.getenv("MOVIDESK_THROTTLE", "7"))  # 10/min => ~6s

BASE = "https://api.movidesk.com/public/v1"
HEADERS = {"Accept": "application/json"}

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")


def pg() -> psycopg2.extensions.connection:
    return psycopg2.connect(NEON_DSN, sslmode="require")


def select_next_ticket_ids(conn, limit: int) -> List[Tuple[int, dt.datetime]]:
    """
    Seleciona os próximos ticket_ids para processar.
    Critério:
      - se ainda não existe em resolvidos_acoes, pega
      - OU se tickets_resolvidos.last_update > resolvidos_acoes.updated_at, pega (desatualizado)
    Ordena por last_resolved_at (quando houver), depois last_update e ticket_id.
    """
    sql = """
    select
        t.ticket_id,
        t.last_update
    from visualizacao_resolvidos.tickets_resolvidos t
    left join visualizacao_resolvidos.resolvidos_acoes ra
           on ra.ticket_id = t.ticket_id
    where
          ra.ticket_id is null
       or t.last_update > coalesce(ra.updated_at, timestamp 'epoch')
    order by
        t.last_resolved_at nulls last,
        t.last_update,
        t.ticket_id
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return rows  # [(ticket_id, last_update), ...]


def req(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper requests.get com erros explícitos."""
    resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
    # Se for erro de rota normal para "tickets" tente /tickets/past (trataremos fora)
    resp.raise_for_status()
    return resp.json()


def fetch_actions_for_ticket(ticket_id: int, use_past: bool = False) -> Dict[str, Any]:
    """
    Faz GET por ID com expand de ações (attachments + timeAppointments).
    Retorna o JSON do ticket (contendo .actions[...]).
    """
    url = f"{BASE}/tickets/{ticket_id}" if not use_past else f"{BASE}/tickets/past/{ticket_id}"

    expand = (
        "actions("
        "$orderby=createdDate asc;"
        "$select=id,isPublic,origin,createdDate,description,attachments,timeAppointments;"
        "$expand="
        "attachments($select=id,fileName,link),"
        "timeAppointments($select=id,activity,date,periodStart,periodEnd,workTime,accountedTime,workTypeName,createdBy,createdByTeam)"
        ")"
    )

    params = {
        "token": API_TOKEN,
        "$select": "id,lastUpdate",
        "$expand": expand,
    }
    return req(url, params)


def fetch_actions_resilient(ticket_id: int, last_update: dt.datetime) -> Dict[str, Any]:
    """
    Estratégia:
      1) tenta rota normal /tickets/{id}
      2) se der 400/404 ou se o ticket for muito antigo (> ~90 dias), tenta /tickets/past/{id}
    """
    now = dt.datetime.utcnow()
    is_old = False
    try:
        # Heurística: se o last_update do ticket for mais antigo que 90 dias, já usa /past
        if isinstance(last_update, dt.datetime):
            is_old = (now - last_update).days > 90
        if is_old:
            return fetch_actions_for_ticket(ticket_id, use_past=True)

        # tenta a rota normal
        return fetch_actions_for_ticket(ticket_id, use_past=False)

    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", 0)
        if status in (400, 404):
            # tenta /past
            return fetch_actions_for_ticket(ticket_id, use_past=True)
        raise


UPSERT = """
insert into visualizacao_resolvidos.resolvidos_acoes (ticket_id, acoes, updated_at)
values (%s::int, %s::jsonb, now())
on conflict (ticket_id) do update
set acoes = excluded.acoes,
    updated_at = now()
"""


def upsert(conn, ticket_id: int, actions_json: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(UPSERT, (ticket_id, json.dumps(actions_json)))
    conn.commit()


def main() -> None:
    conn = pg()
    psycopg2.extras.register_default_jsonb(conn)

    try:
        to_do = select_next_ticket_ids(conn, BATCH_SIZE)
        if not to_do:
            print("Nada a fazer (tudo sincronizado).")
            return

        print(f"Processando {len(to_do)} ticket(s): {[r[0] for r in to_do]}")

        for i, (tid, last_up) in enumerate(to_do, start=1):
            t0 = time.time()
            try:
                data = fetch_actions_resilient(tid, last_up or dt.datetime.utcnow())
                # Normaliza para guardar somente a parte de ações (economiza espaço)
                actions = data.get("actions", [])
                payload = {"ticket_id": tid, "actions": actions}
                upsert(conn, tid, payload)
                print(f"[{i}/{len(to_do)}] ticket {tid}: {len(actions)} ação(ões)")

            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", 0)
                body = e.response.text if getattr(e, "response", None) else ""
                print(f"[ERRO] ticket {tid} HTTP {status}: {body[:200]}")
            except Exception as e:
                print(f"[ERRO] ticket {tid}: {e}")

            # throttle para respeitar 10/min
            dt_exec = time.time() - t0
            if THROTTLE > 0:
                sleep_for = max(0.0, THROTTLE - dt_exec)
                time.sleep(sleep_for)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
