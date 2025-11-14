#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import logging
from typing import Any, Dict, List, Iterable

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s  %(message)s"
)

# ==================== Config ====================
API_BASE   = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN  = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN   = os.getenv("NEON_DSN")

SCHEMA_AUD = "visualizacao_resolvidos"
SCHEMA_RPS = "visualizacao_resolucao"

# Lê da fila por “páginas”
AUDIT_LIMIT   = int(os.getenv("AUDIT_LIMIT",   "300"))  # por rodada
# Se BATCH_SIZE > 0, processa só esse tanto e termina; se <=0, consome tudo o que houver
BATCH_SIZE    = int(os.getenv("BATCH_SIZE",    "0"))
# Tamanho do grupo de IDs por chamada OData (evita MaxNodeCount)
IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))
# Espaço entre chamadas (throttling amigável)
THROTTLE_SEC   = float(os.getenv("THROTTLE_SEC", "0.35"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

# ==================== HTTP helpers ====================

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/resolucao-status"})

def _sleep_retry_after(r: requests.Response) -> bool:
    """Respeita Retry-After se existir; retorna True se dormiu."""
    try:
        ra = r.headers.get("Retry-After")
        if not ra:
            return False
        time.sleep(max(1, int(str(ra).strip())))
        return True
    except Exception:
        return False

def od_get(params: Dict[str, Any], max_retries: int = 5) -> Any:
    """Chamada OData com backoff/Retry-After e mensagens úteis de erro."""
    p = dict(params or {})
    p["token"] = API_TOKEN
    for i in range(max_retries):
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
        # erro “duro”
        url = r.url.replace(API_TOKEN, "***") if API_TOKEN else r.url
        raise requests.HTTPError(f"[HTTP {r.status_code}] {url} :: {r.text[:800]}", response=r)
    r.raise_for_status()  # última tentativa

# ==================== DB helpers ====================

def get_conn():
    return psycopg2.connect(NEON_DSN)

def get_audit_page(conn, limit_: int) -> List[int]:
    """
    Busca um 'page' de IDs da fila, priorizando os maiores ticket_id
    (e em empate, o run mais recente).
    """
    sql = f"""
        select ticket_id
          from {SCHEMA_AUD}.audit_recent_missing
         where table_name = 'resolucao_por_status'
         order by ticket_id desc, run_id desc
         limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        return [r[0] for r in cur.fetchall() or []]

def clear_audit_ids(conn, ids: List[int]) -> None:
    if not ids:
        return
    sql = f"""
        delete from {SCHEMA_AUD}.audit_recent_missing
         where table_name = 'resolucao_por_status'
           and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()

def heartbeat_sync(conn, name="resolucao_por_status") -> None:
    """
    Marca um 'heartbeat' em visualizacao_resolucao.sync_cntrol (nome do usuário).
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            create table if not exists {SCHEMA_RPS}.sync_cntrol(
                name text primary key,
                last_update timestamp not null,
                key text,
                value text
            )
        """)
        cur.execute(f"""
            insert into {SCHEMA_RPS}.sync_cntrol(name,last_update)
            values (%s, now())
            on conflict (name) do update
                set last_update = excluded.last_update
        """, (name,))
    conn.commit()

# ==================== Movidesk fetch ====================

def chunked(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def build_expand():
    # Puxa histórico de status com agente e possíveis times
    return (
        "statusHistories("
        "$select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;"
        "$expand=changedBy($select=id,businessName;$expand=teams($select=businessName))"
        ")"
    )

def fetch_chunk(ids_chunk: List[int]) -> List[Dict[str, Any]]:
    if not ids_chunk:
        return []
    filt = " or ".join([f"id eq {int(i)}" for i in ids_chunk])
    params = {
        "$select": "id,ownerTeam",
        "$expand": build_expand(),
        "$filter": filt,
        "$top": 100
    }
    data = od_get(params) or []
    # Movidesk pode retornar 1 objeto isolado em alguns casos improváveis
    return data if isinstance(data, list) else [data]

# ==================== Transform ====================

GENERIC_TEAMS = {
    "administradores","agente administrador","administrators","agent administrator",
    "default","geral","todos","all","users","usuários","colaboradores"
}
TEAM_PRIORITY = ["telefone","chat","n1","n2","cs","suporte","service desk","desenvolvimento","squad","projeto"]

def _owner_team_name(ticket: Dict[str, Any]) -> str:
    ot = ticket.get("ownerTeam")
    if isinstance(ot, dict):
        return (ot.get("businessName") or "").strip()
    if isinstance(ot, str):
        return ot.strip()
    return ""

def _pick_team(changed_by: Dict[str, Any], owner_team: str, changed_by_team: Any) -> str:
    # 1) se statusHistory trouxe changedByTeam, usa
    if isinstance(changed_by_team, dict):
        n = (changed_by_team.get("businessName") or "").strip()
        if n:
            return n
    if isinstance(changed_by_team, str):
        n = changed_by_team.strip()
        if n:
            return n

    # 2) tenta pelos teams do agente (exclui genéricos, prioriza por palavras-chave)
    names: List[str] = []
    if isinstance(changed_by, dict):
        teams = changed_by.get("teams")
        if isinstance(teams, list):
            for t in teams:
                n = (t.get("businessName") if isinstance(t, dict) else (t if isinstance(t, str) else "")) or ""
                n = n.strip()
                low = n.lower()
                if n and low not in GENERIC_TEAMS and not low.startswith("admin"):
                    names.append(n)

    names = list(dict.fromkeys(names))
    if not names and isinstance(changed_by, dict):
        teams = changed_by.get("teams")
        if isinstance(teams, list):
            for t in teams:
                n = (t.get("businessName") if isinstance(t, dict) else (t if isinstance(t, str) else "")) or ""
                n = n.strip()
                if n:
                    names.append(n)
        names = list(dict.fromkeys(names))

    if names:
        lowered = [n.lower() for n in names]
        for key in TEAM_PRIORITY:
            for i, low in enumerate(lowered):
                if key in low:
                    return names[i]
        return names[0]

    # 3) fallback para a equipe do ticket
    return owner_team or ""

def map_rows(ticket: Dict[str, Any]) -> List[tuple]:
    """
    Produz linhas para upsert:
      (ticket_id, status, justificativa, seconds_uti, permanency_time_fulltime_seconds,
       changed_by_json, changed_date, agent_name, team_name, time_squad)
    """
    tid = int(ticket.get("id"))
    owner_team = _owner_team_name(ticket)
    out: List[tuple] = []

    for h in ticket.get("statusHistories") or []:
        status = h.get("status") or ""
        justificativa = h.get("justification") or ""
        sec_work = int(h.get("permanencyTimeWorkingTime") or 0)
        sec_full = float(h.get("permanencyTimeFullTime") or 0.0)
        changed_by = h.get("changedBy") or {}
        agent = changed_by.get("businessName") if isinstance(changed_by, dict) else ""
        team = _pick_team(changed_by, owner_team, h.get("changedByTeam"))
        changed_date = h.get("changedDate")
        # time_squad: mantemos vazio; triggers podem preencher (se houver)
        out.append((
            tid, status, justificativa, sec_work, sec_full,
            json.dumps(changed_by, ensure_ascii=False),
            changed_date, agent or "", team or "", None  # time_squad
        ))
    return out

def dedupe(rows: List[tuple]) -> List[tuple]:
    seen = set()
    out: List[tuple] = []
    for r in rows:
        key = (r[0], r[1], r[2], r[6])  # ticket_id, status, justificativa, changed_date
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# ==================== Upsert ====================

UPSERT_SQL = f"""
insert into {SCHEMA_RPS}.resolucao_por_status
(ticket_id, status, justificativa, seconds_uti, permanency_time_fulltime_seconds,
 changed_by, changed_date, agent_name, team_name, time_squad)
values %s
on conflict (ticket_id, status, justificativa, changed_date) do update set
  seconds_uti                         = excluded.seconds_uti,
  permanency_time_fulltime_seconds    = excluded.permanency_time_fulltime_seconds,
  changed_by                          = excluded.changed_by,
  agent_name                          = excluded.agent_name,
  team_name                           = excluded.team_name,
  time_squad                          = excluded.time_squad,
  imported_at                         = now()
"""

def upsert_rows(conn, rows: List[tuple]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()

# ==================== Main loop ====================

def process_ids(conn, ids: List[int]) -> int:
    """
    Busca tickets em grupos (maiores IDs primeiro já vem do SELECT),
    grava linhas e limpa da audit somente os IDs que vieram com payload.
    """
    if not ids:
        return 0

    group = max(1, IDS_GROUP_SIZE)
    processed_ids: List[int] = []
    total_rows = 0

    i = 0
    while i < len(ids):
        part = ids[i:i+group]
        try:
            data = fetch_chunk(part)
        except requests.HTTPError as e:
            # se vier erro de complexidade (ex.: MaxNodeCount), reduz o grupo
            if group > 1 and any(code in str(e).lower() for code in ["maxnodecount", "request too long", "odata"]):
                group = max(1, group // 2)
                logging.warning("Reducing group size to %s due to OData limits", group)
                continue
            else:
                logging.warning("Erro HTTP duro em fetch_chunk: %s", e)
                # avança mesmo assim pra não travar na mesma página
                i += len(part)
                continue

        rows_all: List[tuple] = []
        got_tids = set()

        for t in data or []:
            try:
                tid = int(t.get("id"))
                got_tids.add(tid)
                rows = dedupe(map_rows(t))
                rows_all.extend(rows)
            except Exception as ex:
                logging.warning("Falha ao mapear ticket: %s :: %s", t.get("id"), ex)

        if rows_all:
            upsert_rows(conn, rows_all)
            total_rows += len(rows_all)

        if got_tids:
            clear_audit_ids(conn, sorted(got_tids, reverse=True))
            processed_ids.extend(sorted(got_tids, reverse=True))

        i += len(part)

    if processed_ids:
        logging.info("Processados (e limpos da audit): %d tickets", len(processed_ids))
    return total_rows

def main():
    total_upsert = 0
    with get_conn() as conn:
        heartbeat_sync(conn)  # cria tabela se preciso e marca heartbeat
        ilimitado = (BATCH_SIZE <= 0)

        while True:
            page_limit = AUDIT_LIMIT if ilimitado else min(AUDIT_LIMIT, BATCH_SIZE)
            ids = get_audit_page(conn, page_limit)

            if not ids:
                logging.info("Fila vazia para 'resolucao_por_status'. Nada a fazer.")
                break

            if not ilimitado and len(ids) > BATCH_SIZE:
                ids = ids[:BATCH_SIZE]

            logging.info("Lote da fila (maiores IDs primeiro): %s%s",
                         ids[:10], " ..." if len(ids) > 10 else "")
            total_upsert += process_ids(conn, ids)

            if not ilimitado:
                break  # modo 'um único lote'

            # loop até esvaziar a audit ou não gerar mais nada
            if len(ids) < page_limit:
                break

        heartbeat_sync(conn)  # marca finalização do ciclo

    logging.info("FIM. Linhas upsertadas: %d", total_upsert)

if __name__ == "__main__":
    main()
