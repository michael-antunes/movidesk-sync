# -*- coding: utf-8 -*-
"""
Consome IDs da visualizacao_resolvidos.audit_recent_missing (table_name='resolucao_por_status'),
busca statusHistories no Movidesk e upserta em visualizacao_resolucao.resolucao_por_status.
Após gravar, remove os IDs processados da audit e marca heartbeat em
visualizacao_resolucao.sync_control (name='resolucao_por_status').
"""

import os
import time
import json
import logging
from typing import Any, Dict, List, Iterable

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

# ========= Config =========
API_BASE     = "https://api.movidesk.com/public/v1/tickets"
TOKEN        = os.environ["MOVIDESK_TOKEN"] or os.environ["MOVIDESK_API_TOKEN"]
DSN          = os.environ["NEON_DSN"]

SCHEMA_RESOLVIDOS = "visualizacao_resolvidos"
SCHEMA_RESOLUCAO  = "visualizacao_resolucao"

# tamanhos e limites
IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))     # evita MaxNodeCount=100 do OData
BATCH_LIMIT    = int(os.getenv("BATCH_LIMIT", "300"))       # quantos IDs da audit por execução
THROTTLE_SEC   = float(os.getenv("THROTTLE_SEC", "0.35"))   # pausa leve entre chamadas
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "4"))

# ==========================

def _sleep_retry_after(resp: requests.Response) -> bool:
    try:
        ra = resp.headers.get("Retry-After")
        if not ra:
            return False
        time.sleep(max(1, int(str(ra).strip())))
        return True
    except Exception:
        return False

def req(params: Dict[str, Any]) -> Any:
    """GET com tolerância a 429/5xx e log amigável (sem vazar token)."""
    p = dict(params or {})
    p["token"] = TOKEN
    for i in range(MAX_RETRIES):
        r = requests.get(API_BASE, params=p, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_retry_after(r):
                continue
            time.sleep(1 + 2*i)
            continue
        # erro “duro”
        full = r.url.replace(TOKEN, "***") if TOKEN else r.url
        body = (r.text or "")[:2000]
        raise requests.HTTPError(f"[HTTP {r.status_code}] {full}\n{body}", response=r)
    # se saiu do laço ainda com erro, levanta
    r.raise_for_status()

def ensure_structure(conn) -> None:
    """Garante que as tabelas-alvo existam. Não altera estrutura se já existe."""
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {SCHEMA_RESOLUCAO}")
        cur.execute(f"""
            create table if not exists {SCHEMA_RESOLUCAO}.resolucao_por_status(
                ticket_id integer not null,
                status text not null,
                justificativa text not null,
                seconds_uti integer,
                permanency_time_fulltime_seconds double precision,
                changed_by jsonb,
                changed_date timestamp,
                imported_at timestamp default now(),
                agent_name text default '',
                team_name text default '',
                time_squad text,
                primary key (ticket_id, status, justificativa, changed_date)
            )
        """)
        cur.execute(f"""
            create table if not exists {SCHEMA_RESOLUCAO}.sync_control(
                name text primary key,
                last_update timestamp not null default now(),
                key text,
                value text
            )
        """)
    conn.commit()

def get_audit_ids(conn, limit_: int) -> List[int]:
    """Busca IDs únicos da audit (somente resolucao_por_status), ordenados do maior para o menor (mais novos primeiro)."""
    sql = f"""
        select distinct ticket_id
          from {SCHEMA_RESOLVIDOS}.audit_recent_missing
         where table_name = 'resolucao_por_status'
         order by ticket_id desc
         limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall() or []
    ids = [int(r[0]) for r in rows]
    logging.info("IDs na audit (resolucao_por_status): %d", len(ids))
    return ids

def chunked(lst: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# --------- Mapeamento dos campos ----------
def _pick_team_name(owner_team: str, changed_by: Dict[str, Any], changed_by_team: Any) -> str:
    """Heurística simples para escolher o nome da equipe."""
    # 1) usar changedByTeam se vier como string ou dict
    if isinstance(changed_by_team, str):
        n = changed_by_team.strip()
        if n:
            return n
    if isinstance(changed_by_team, dict):
        n = (changed_by_team.get("businessName") or "").strip()
        if n:
            return n

    # 2) tentar nas teams do changed_by
    teams = []
    if isinstance(changed_by, dict):
        arr = changed_by.get("teams") or []
        if isinstance(arr, list):
            for t in arr:
                if isinstance(t, dict):
                    n = (t.get("businessName") or "").strip()
                elif isinstance(t, str):
                    n = t.strip()
                else:
                    n = ""
                if n:
                    teams.append(n)

    if teams:
        return teams[0]

    # 3) fallback: owner_team do ticket (string)
    return owner_team or ""

def _pick_time_squad(changed_by: Dict[str, Any]) -> str:
    """Extrai algo com 'squad' no nome das equipes do agente, se existir."""
    if not isinstance(changed_by, dict):
        return ""
    arr = changed_by.get("teams") or []
    if not isinstance(arr, list):
        return ""
    for t in arr:
        n = ""
        if isinstance(t, dict):
            n = (t.get("businessName") or "").strip()
        elif isinstance(t, str):
            n = t.strip()
        if n and "squad" in n.lower():
            return n
    return ""

def map_histories(ticket: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converte statusHistories do ticket em linhas para resolucao_por_status.
    """
    tid = int(ticket.get("id"))
    # ownerTeam pode vir string ou objeto
    owner_team = ""
    ot = ticket.get("ownerTeam")
    if isinstance(ot, str):
        owner_team = ot
    elif isinstance(ot, dict):
        owner_team = (ot.get("businessName") or "").strip()

    out = []
    for h in ticket.get("statusHistories") or []:
        status = h.get("status") or ""
        justificativa = h.get("justification") or ""
        sec_work = int(h.get("permanencyTimeWorkingTime") or 0)
        sec_full = float(h.get("permanencyTimeFullTime") or 0)
        changed_by = h.get("changedBy") or {}
        changed_dt = h.get("changedDate")  # string ISO

        agent_name = ""
        if isinstance(changed_by, dict):
            agent_name = (changed_by.get("businessName") or "").strip()

        team_name = _pick_team_name(owner_team, changed_by, h.get("changedByTeam"))
        time_squad = _pick_time_squad(changed_by)

        out.append({
            "ticket_id": tid,
            "status": status,
            "justificativa": justificativa,
            "seconds_uti": sec_work,
            "permanency_time_fulltime_seconds": sec_full,
            "changed_by": changed_by,
            "changed_date": changed_dt,
            "agent_name": agent_name,
            "team_name": team_name,
            "time_squad": time_squad,
        })
    return out

# --------- Fetch ----------
SELECT_FIELDS = "id,ownerTeam"  # campos do ticket
EXPAND_FIELDS = (
    "statusHistories("
    "$select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;"
    "$expand=changedBy($select=id,businessName;$expand=teams($select=businessName))"
    ")"
)

def fetch_group(ids: List[int]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    filtro = " or ".join([f"id eq {int(i)}" for i in ids])
    params = {
        "$select": SELECT_FIELDS,
        "$expand": EXPAND_FIELDS,
        "$filter": filtro,
        "$top": 100
    }
    data = req(params) or []
    time.sleep(THROTTLE_SEC)
    return data

# --------- Upsert / limpeza ----------
def upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    payload = [(
        r["ticket_id"], r["status"], r["justificativa"],
        r["seconds_uti"], r["permanency_time_fulltime_seconds"],
        json.dumps(r["changed_by"], ensure_ascii=False),
        r["changed_date"], r["agent_name"], r["team_name"], r["time_squad"]
    ) for r in rows]

    sql = f"""
        insert into {SCHEMA_RESOLUCAO}.resolucao_por_status
          (ticket_id, status, justificativa, seconds_uti,
           permanency_time_fulltime_seconds, changed_by, changed_date,
           agent_name, team_name, time_squad)
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
    with conn.cursor() as cur:
        execute_values(cur, sql, payload, page_size=200)
    conn.commit()
    return len(rows)

def clear_audit(conn, ids: List[int]) -> None:
    if not ids:
        return
    sql = f"""
        delete from {SCHEMA_RESOLVIDOS}.audit_recent_missing
         where table_name = 'resolucao_por_status'
           and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()

def heartbeat(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            insert into {SCHEMA_RESOLUCAO}.sync_control (name, last_update)
            values ('resolucao_por_status', now())
            on conflict (name) do update set last_update = excluded.last_update
        """)
    conn.commit()

# --------- Main ----------
def main():
    with psycopg2.connect(DSN) as conn:
        ensure_structure(conn)

        ids = get_audit_ids(conn, BATCH_LIMIT)
        if not ids:
            logging.info("Nada para fazer (audit vazia para resolucao_por_status).")
            heartbeat(conn)
            return

        total_upserts = 0
        processed_ids: List[int] = []

        for group in chunked(ids, IDS_GROUP_SIZE):
            try:
                data = fetch_group(group)
                rows_all: List[Dict[str, Any]] = []
                for t in data or []:
                    rows_all.extend(map_histories(t))

                if rows_all:
                    total_upserts += upsert_rows(conn, rows_all)

                # IDs efetivamente retornados pela API (para limpar audit com segurança)
                returned = []
                for t in data or []:
                    try:
                        returned.append(int(t.get("id")))
                    except Exception:
                        pass

                if returned:
                    processed_ids.extend(returned)

            except requests.HTTPError as e:
                logging.warning("Falha no grupo %s: %s", group, e)

        if processed_ids:
            clear_audit(conn, list(set(processed_ids)))
        heartbeat(conn)
        logging.info("Finalizado. Linhas upsertadas: %d  |  IDs limpos da audit: %d",
                     total_upserts, len(set(processed_ids)))

if __name__ == "__main__":
    main()
