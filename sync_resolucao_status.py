#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
from typing import Any, Dict, List, Iterable

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

API_BASE = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

# ------- controles -------
IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))   # grupo no $filter (MaxNodeCount ~100)
BATCH_LIMIT    = int(os.getenv("BATCH_LIMIT", "300"))     # quantos IDs por execução
THROTTLE_SEC   = float(os.getenv("THROTTLE_SEC", "0.35")) # pausa entre chamadas
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "4"))
# -------------------------

SCHEMA_RES   = "visualizacao_resolvidos"
SCHEMA_DET   = "visualizacao_resolucao"
TBL_AUDIT    = f"{SCHEMA_RES}.audit_recent_missing"
TBL_STATUS   = f"{SCHEMA_DET}.resolucao_por_status"

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/resolucao_por_status"})

# --------------------------------------------------------
# OData params (SEMPRE sem changedByTeam/teams)
# --------------------------------------------------------
SELECT_FIELDS = "id,ownerTeam"
EXPAND_EXPR   = (
    "statusHistories("
    "$select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate;"
    "$expand=changedBy($select=id,businessName)"
    ")"
)
logging.info("EXPAND usado (debug): %s", EXPAND_EXPR)

# --------------------------------------------------------
# Utils
# --------------------------------------------------------
def chunked(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _sleep_retry_after(r: requests.Response) -> bool:
    try:
        ra = r.headers.get("retry-after")
        if not ra:
            return False
        time.sleep(max(1, int(str(ra).strip())))
        return True
    except Exception:
        return False

def md_get(params: Dict[str, Any]) -> Any:
    """GET com retentativas para 429/5xx; 4xx ≠ 429 estoura (log seguro)."""
    p = dict(params or {})
    p["token"] = API_TOKEN
    last_err = None
    for i in range(MAX_RETRIES):
        r = SESS.get(API_BASE, params=p, timeout=60)
        if r.status_code == 200:
            try:
                return r.json()
            finally:
                time.sleep(THROTTLE_SEC)
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_retry_after(r):
                continue
            time.sleep(min(60, 1 + 2*i))
            continue
        last_err = r
        break
    if last_err is None:
        r.raise_for_status()
    try:
        safe_url = last_err.url.replace(API_TOKEN, "***")
    except Exception:
        safe_url = API_BASE
    logging.warning("Erro HTTP duro em fetch_chunk: [HTTP %s] %s :: %s",
                    last_err.status_code, safe_url, last_err.text)
    last_err.raise_for_status()

# --------------------------------------------------------
# Audit: pegar IDs (maiores -> menores) e limpar
# --------------------------------------------------------
def get_audit_ids(conn, limit_: int) -> List[int]:
    sql = f"""
      select distinct ticket_id
      from {TBL_AUDIT}
      where table_name = 'resolucao_por_status'
      order by ticket_id desc
      limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall() or []
    return [r[0] for r in rows]

def clear_audit_ids(conn, ids: List[int]) -> None:
    if not ids:
        return
    sql = f"""
      delete from {TBL_AUDIT}
      where table_name = 'resolucao_por_status'
        and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()

# --------------------------------------------------------
# Movidesk fetch (SEM changedByTeam)
# --------------------------------------------------------
def fetch_chunk(ids_chunk: List[int]) -> List[Dict[str, Any]]:
    if not ids_chunk:
        return []
    filtro = " or ".join([f"id eq {int(i)}" for i in ids_chunk])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND_EXPR,
        "$filter": filtro,
        "$top": 100,
        "includeDeletedItems": "true",
    }
    data = md_get(params)
    return data or []

# --------------------------------------------------------
# Mapping helpers
# --------------------------------------------------------
def _name_from_obj_or_str(x) -> str:
    if isinstance(x, dict):
        return (x.get("businessName") or x.get("name") or "").strip()
    if isinstance(x, str):
        return x.strip()
    return ""

def _owner_team_name(ticket: Dict[str, Any]) -> str:
    return _name_from_obj_or_str(ticket.get("ownerTeam"))

def extract_rows(ticket: Dict[str, Any]) -> List[tuple]:
    tid = int(ticket.get("id"))
    owner_team = _owner_team_name(ticket)  # campo seguro para equipe
    rows = []
    for h in (ticket.get("statusHistories") or []):
        changed_date = h.get("changedDate")
        if not changed_date:
            continue  # PK inclui changed_date
        status = h.get("status") or ""
        justificativa = h.get("justification") or ""
        sec_work = int(h.get("permanencyTimeWorkingTime") or 0)
        sec_full = float(h.get("permanencyTimeFullTime") or 0.0)
        changed_by = h.get("changedBy") or {}
        agent_name = ""
        if isinstance(changed_by, dict):
            agent_name = (changed_by.get("businessName") or "").strip()

        team_name = owner_team    # sem changedByTeam disponível
        time_squad = None         # mantenha nulo/'' se não houver regra

        rows.append((
            tid, status, justificativa, sec_work, sec_full,
            json.dumps(changed_by, ensure_ascii=False),
            changed_date, agent_name, team_name, time_squad
        ))
    return rows

# --------------------------------------------------------
# UPSERT
# --------------------------------------------------------
UPSERT_SQL = f"""
insert into {TBL_STATUS}
(ticket_id, status, justificativa, seconds_uti, permanency_time_fulltime_seconds,
 changed_by, changed_date, agent_name, team_name, time_squad)
values %s
on conflict (ticket_id, status, justificativa, changed_date) do update set
  seconds_uti = excluded.seconds_uti,
  permanency_time_fulltime_seconds = excluded.permanency_time_fulltime_seconds,
  changed_by = excluded.changed_by,
  agent_name = excluded.agent_name,
  team_name  = excluded.team_name,
  time_squad = excluded.time_squad,
  imported_at = now()
"""

def upsert_rows(conn, rows: List[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()

# --------------------------------------------------------
# Main
# --------------------------------------------------------
def main():
    with psycopg2.connect(NEON_DSN) as conn:
        ids = get_audit_ids(conn, BATCH_LIMIT)
        if not ids:
            logging.info("Fila vazia para resolucao_por_status. Nada a fazer.")
            return

        logging.info("Lote da fila (maiores IDs primeiro): %s ...", ids[:10])

        total_rows = 0
        processed_for_clear: List[int] = []

        for grp in chunked(ids, IDS_GROUP_SIZE):
            try:
                data = fetch_chunk(grp)
            except requests.HTTPError:
                # grupo com erro duro: segue para o próximo
                continue

            if not data:
                continue

            rows_to_upsert: List[tuple] = []
            got_ids: List[int] = []

            for ticket in data:
                try:
                    tid = int(ticket.get("id"))
                except Exception:
                    continue
                trows = extract_rows(ticket)
                if trows:
                    rows_to_upsert.extend(trows)
                    got_ids.append(tid)

            if rows_to_upsert:
                upsert_rows(conn, rows_to_upsert)
                total_rows += len(rows_to_upsert)

            if got_ids:
                processed_for_clear.extend(got_ids)

        if processed_for_clear:
            processed_for_clear = sorted(list(set(processed_for_clear)))
            clear_audit_ids(conn, processed_for_clear)

        logging.info("Finalizado. Linhas upsertadas: %d | IDs limpos da audit: %d",
                     total_rows, len(processed_for_clear))

if __name__ == "__main__":
    main()
