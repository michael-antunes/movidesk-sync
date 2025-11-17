#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Varredura por faixa (range scan) e envio para audit_recent_missing (tickets_resolvidos).

- Controlada por visualizacao_resolvidos.range_scan_control
- Sempre semeia data_fim = 2018-01-01 00:00:00+00
- Garante trigger que, ao alcançar o fim da janela (ultima_data_validada == data_fim),
  reinicia a janela colocando data_inicio = now() e ultima_data_validada = now().

ENV:
  NEON_DSN
  MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN)
  RANGE_SCAN_LIMIT       (default 400)   -> qtd máx. de tickets a empilhar por execução
  RANGE_SCAN_THROTTLE    (default 0.25s) -> pausa entre páginas da API
  RESET_RANGE_NOW        (default false) -> se "1/true/yes": faz reset imediato da janela (data_inicio=now, ultima=now)
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values

# -------------------- CONFIG --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s  %(message)s"
)

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN   = os.getenv("NEON_DSN")

PAGE_TOP = 100
LIMIT = int(os.getenv("RANGE_SCAN_LIMIT", "400"))
THROTTLE = float(os.getenv("RANGE_SCAN_THROTTLE", "0.25"))

RESET_RANGE_NOW = (os.getenv("RESET_RANGE_NOW", "0").lower() in ("1", "true", "yes"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

# -------------------- HELPERS --------------------

def z(dt: datetime) -> str:
    """Formata datetime UTC em literal OData (yyyy-mm-ddTHH:MM:SSZ)."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_dt(x) -> Optional[datetime]:
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def conn():
    return psycopg2.connect(DSN)

# -------------------- DDL / TRIGGER --------------------

BOUNCE_FN_SQL = """
create or replace function visualizacao_resolvidos.trg_range_scan_bounce()
returns trigger
language plpgsql
as $$
begin
  -- Quando a varredura chega exatamente no fim da janela, reinicia:
  -- data_inicio = agora e ultima_data_validada = agora.
  if NEW.ultima_data_validada is not distinct from NEW.data_fim then
    NEW.data_inicio := now();
    NEW.ultima_data_validada := now();
  end if;
  return NEW;
end;
$$;
"""

BOUNCE_TRG_SQL = """
drop trigger if exists trg_range_scan_bounce on visualizacao_resolvidos.range_scan_control;

create trigger trg_range_scan_bounce
before update on visualizacao_resolvidos.range_scan_control
for each row
execute function visualizacao_resolvidos.trg_range_scan_bounce();
"""

def ensure_table_and_trigger():
    with conn() as c, c.cursor() as cur:
        # schema + tabela + check
        cur.execute("create schema if not exists visualizacao_resolvidos;")
        cur.execute("""
            create table if not exists visualizacao_resolvidos.range_scan_control(
              data_fim              timestamptz not null,
              data_inicio           timestamptz not null,
              ultima_data_validada  timestamptz,
              constraint ck_range_scan_bounds check (
                data_inicio is not null
                and data_fim    is not null
                and data_inicio <> data_fim
                and (
                  ultima_data_validada is null
                  or (
                    ultima_data_validada >= least(data_inicio, data_fim)
                    and ultima_data_validada <= greatest(data_inicio, data_fim)
                  )
                )
              )
            )
        """)
        # semente (uma única linha)
        cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
        if cur.fetchone()[0] == 0:
            logging.info("Sem linha em range_scan_control; inserindo semente (2018-01-01 / now / now).")
            cur.execute("""
                insert into visualizacao_resolvidos.range_scan_control
                  (data_fim, data_inicio, ultima_data_validada)
                values (timestamptz '2018-01-01 00:00:00+00', now(), now())
            """)

        # trigger de “rebate”
        cur.execute(BOUNCE_FN_SQL)
        cur.execute(BOUNCE_TRG_SQL)
        c.commit()

def reset_window_now():
    """Força reset imediato da janela (data_inicio=now, ultima_data_validada=now). Mantém data_fim = 2018-01-01."""
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            update visualizacao_resolvidos.range_scan_control
               set data_inicio = now(),
                   ultima_data_validada = now(),
                   data_fim = timestamptz '2018-01-01 00:00:00+00'
        """)
        c.commit()
    logging.info("RESET_RANGE_NOW: janela reiniciada (data_inicio=now, ultima=now, data_fim=2018-01-01).")

# -------------------- API --------------------

def md_get(params: dict, max_retries: int = 5):
    last = None
    for i in range(max_retries):
        r = requests.get(API, params=params, timeout=60)
        if r.status_code == 200:
            return r.json() or []
        if r.status_code in (429, 500, 502, 503, 504):
            # backoff simples + retry-after
            ra = r.headers.get("retry-after")
            if ra:
                try:
                    time.sleep(max(1, int(str(ra).strip())))
                except Exception:
                    time.sleep(1 + i)
            else:
                time.sleep(1 + 2*i)
            last = r
            continue
        # erro "duro"
        r.raise_for_status()
    if last is not None:
        last.raise_for_status()
    return []  # nunca chega aqui

# -------------------- LÓGICA PRINCIPAL --------------------

def fetch_ids_in_range(dt_start: datetime, dt_end: datetime) -> Tuple[List[int], Optional[datetime]]:
    """
    Busca IDs pela API, filtrando por lastUpdate entre dt_start e dt_end,
    mas valida o "evento" verdadeiro pela baseStatus:
      - Resolved/Closed => resolvedIn/closedIn
      - Canceled        => canceledIn
    Retorna (ids, menor_data_de_evento_encontrada).
    """
    ids: List[int] = []
    min_evt: Optional[datetime] = None

    skip = 0
    while len(ids) < LIMIT:
        params = {
            "token": TOKEN,
            "$select": "id,baseStatus,resolvedIn,closedIn,canceledIn,lastUpdate",
            "$filter": f"lastUpdate ge {z(dt_start)} and lastUpdate le {z(dt_end)}",
            "$orderby": "lastUpdate desc",
            "$top": PAGE_TOP,
            "$skip": skip
        }
        page = md_get(params)
        if not page:
            break

        for t in page:
            bs = (t.get("baseStatus") or "").strip()
            # determina o timestamp "eventual" que nos interessa
            ev = None
            if bs in ("Resolved", "Closed"):
                ev = to_dt(t.get("resolvedIn")) or to_dt(t.get("closedIn"))
            elif bs == "Canceled":
                ev = to_dt(t.get("canceledIn"))

            if not ev:
                continue

            # garante que o evento cai no range (robustez extra)
            if ev < dt_start or ev > dt_end:
                continue

            try:
                tid = int(t.get("id"))
            except Exception:
                continue

            if tid not in ids:
                ids.append(tid)
            if (min_evt is None) or (ev < min_evt):
                min_evt = ev

            if len(ids) >= LIMIT:
                break

        if len(page) < PAGE_TOP:
            break

        skip += PAGE_TOP
        time.sleep(THROTTLE)

    return ids, min_evt

def upsert_audit_missing(ids: List[int], dt_start: datetime, dt_end: datetime):
    if not ids:
        return

    with conn() as c, c.cursor() as cur:
        # quais já temos na tabela final?
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where ticket_id = any(%s)
        """, (ids,))
        have = {r[0] for r in cur.fetchall()}

        missing = [i for i in ids if i not in have]

        # run_id (cria se não houver)
        cur.execute("select max(id) from visualizacao_resolvidos.audit_recent_run")
        rid = cur.fetchone()[0]
        if rid is None:
            cur.execute("""
                insert into visualizacao_resolvidos.audit_recent_run
                  (window_start, window_end, total_api, missing_total, run_at,
                   window_from, window_to, total_local, notes)
                values (%s, %s, %s, %s, now(), %s, %s, %s, %s)
                returning id
            """, (
                dt_start, dt_end, 0, 0,
                dt_start, dt_end, 0,
                'range-scan-backward'
            ))
            rid = cur.fetchone()[0]

        if missing:
            execute_values(cur, """
                insert into visualizacao_resolvidos.audit_recent_missing
                    (run_id, table_name, ticket_id)
                values %s
                on conflict do nothing
            """, [(rid, "tickets_resolvidos", m) for m in missing])

        c.commit()

def read_control() -> Tuple[datetime, datetime, datetime]:
    """Lê (data_inicio, data_fim, ultima_data_validada)."""
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio)
              from visualizacao_resolvidos.range_scan_control
             limit 1
        """)
        row = cur.fetchone()
        if not row:
            raise RuntimeError("range_scan_control sem linha. (ensure_table_and_trigger não executou?)")
        return row[0], row[1], row[2]

def write_control_next(ultima: datetime):
    """Atualiza ultima_data_validada; a trigger trata o caso de igualdade ao data_fim."""
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            update visualizacao_resolvidos.range_scan_control
               set ultima_data_validada = %s
        """, (ultima,))
        c.commit()

def main():
    ensure_table_and_trigger()

    if RESET_RANGE_NOW:
        reset_window_now()

    data_inicio, data_fim, ultima = read_control()
    logging.info("Janela atual: data_fim=%s | data_inicio=%s | ultima=%s", data_fim, data_inicio, ultima)

    # Se já "rebateu" (ultima <= data_fim), não há o que varrer agora.
    if ultima <= data_fim:
        logging.info("Nada a varrer: ultima_data_validada <= data_fim.")
        return

    ids, min_evt = fetch_ids_in_range(data_fim, ultima)
    logging.info("IDs coletados no range: %d", len(ids))

    upsert_audit_missing(ids, data_fim, ultima)

    # Avança a "ultima_data_validada" para o menor evento encontrado (ou para o próprio data_fim,
    # o que disparará a trigger de “rebate” e reiniciará a janela).
    if min_evt:
        next_ultima = min_evt
    else:
        next_ultima = data_fim

    write_control_next(next_ultima)
    logging.info("Atualizado ultima_data_validada -> %s", next_ultima)

if __name__ == "__main__":
    main()
