# -*- coding: utf-8 -*-
import os, time, json, logging
from typing import Any, Dict, List, Iterable
import requests, psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

API_BASE = "https://api.movidesk.com/public/v1/tickets"
TOKEN    = os.environ["MOVIDESK_TOKEN"] or os.environ["MOVIDESK_API_TOKEN"]
DSN      = os.environ["NEON_DSN"]

SCHEMA_RESOLVIDOS = "visualizacao_resolvidos"
SCHEMA_RESOLUCAO  = "visualizacao_resolucao"

IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))
BATCH_LIMIT    = int(os.getenv("BATCH_LIMIT", "300"))
THROTTLE_SEC   = float(os.getenv("THROTTLE_SEC", "0.35"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "4"))

def _sleep_retry_after(resp: requests.Response) -> bool:
    try:
        ra = resp.headers.get("Retry-After")
        if not ra: return False
        time.sleep(max(1, int(str(ra).strip())))
        return True
    except Exception:
        return False

def req(params: Dict[str, Any]) -> Any:
    p = dict(params or {}); p["token"] = TOKEN
    for i in range(MAX_RETRIES):
        r = requests.get(API_BASE, params=p, timeout=60)
        if r.status_code == 200: return r.json()
        if r.status_code in (429,500,502,503,504):
            if _sleep_retry_after(r): continue
            time.sleep(1 + 2*i); continue
        full = r.url.replace(TOKEN, "***") if TOKEN else r.url
        body = (r.text or "")[:2000]
        raise requests.HTTPError(f"[HTTP {r.status_code}] {full}\n{body}", response=r)
    r.raise_for_status()

def ensure_structure(conn):
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

def get_audit_ids(conn, limit_) -> List[int]:
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
    logging.info("IDs na audit (resolucao_por_status): %d (maior primeiro)", len(ids))
    return ids

def chunked(lst: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _pick_team_name(owner_team, changed_by, changed_by_team) -> str:
    if isinstance(changed_by_team, str):
        n = changed_by_team.strip()
        if n: return n
    if isinstance(changed_by_team, dict):
        n = (changed_by_team.get("businessName") or "").strip()
        if n: return n
    teams = []
    if isinstance(changed_by, dict):
        arr = changed_by.get("teams") or []
        if isinstance(arr, list):
            for t in arr:
                n = (t.get("businessName") if isinstance(t,dict) else (t if isinstance(t,str) else "")) or ""
                n = n.strip()
                if n: teams.append(n)
    return teams[0] if teams else (owner_team or "")

def _pick_time_squad(changed_by) -> str:
    if not isinstance(changed_by, dict): return ""
    arr = changed_by.get("teams") or []
    if not isinstance(arr, list): return ""
    for t in arr:
        n = (t.get("businessName") if isinstance(t,dict) else (t if isinstance(t,str) else "")) or ""
        n = (n or "").strip()
        if n and "squad" in n.lower(): return n
    return ""

def map_histories(ticket: Dict[str, Any]) -> List[Dict[str, Any]]:
    tid = int(ticket.get("id"))
    ot = ticket.get("ownerTeam"); owner_team = (ot if isinstance(ot,str) else (ot or {}).get("businessName") or "").strip()
    out = []
    for h in ticket.get("statusHistories") or []:
        cb = h.get("changedBy") or {}
        out.append({
            "ticket_id": tid,
            "status": h.get("status") or "",
            "justificativa": h.get("justification") or "",
            "seconds_uti": int(h.get("permanencyTimeWorkingTime") or 0),
            "permanency_time_fulltime_seconds": float(h.get("permanencyTimeFullTime") or 0),
            "changed_by": cb,
            "changed_date": h.get("changedDate"),
            "agent_name": (cb.get("businessName") or "").strip() if isinstance(cb,dict) else "",
            "team_name": _pick_team_name(owner_team, cb, h.get("changedByTeam")),
            "time_squad": _pick_time_squad(cb),
        })
    return out

SELECT_FIELDS = "id,ownerTeam"
EXPAND_FIELDS = (
    "statusHistories("
    "$select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate,changedByTeam;"
    "$expand=changedBy($select=id,businessName;$expand=teams($select=businessName))"
    ")"
)

def fetch_group(ids: List[int]) -> List[Dict[str, Any]]:
    if not ids: return []
    filtro = " or ".join([f"id eq {int(i)}" for i in ids])
    params = {"$select": SELECT_FIELDS, "$expand": EXPAND_FIELDS, "$filter": filtro, "$top": 100}
    data = req(params) or []
    # >>> garante processar primeiro os maiores IDs
    try:
        data.sort(key=lambda t: int(t.get("id", 0)), reverse=True)
    except Exception:
        pass
    time.sleep(THROTTLE_SEC)
    return data

def upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows: return 0
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

def clear_audit(conn, ids: List[int]):
    if not ids: return
    sql = f"""
        delete from {SCHEMA_RESOLVIDOS}.audit_recent_missing
         where table_name = 'resolucao_por_status'
           and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()

def heartbeat(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            insert into {SCHEMA_RESOLUCAO}.sync_control (name, last_update)
            values ('resolucao_por_status', now())
            on conflict (name) do update set last_update = excluded.last_update
        """)
    conn.commit()

def main():
    with psycopg2.connect(DSN) as conn:
        ensure_structure(conn)

        ids = get_audit_ids(conn, BATCH_LIMIT)  # já vem DESC
        if not ids:
            logging.info("Nada para fazer (audit vazia).")
            heartbeat(conn); return

        total_upserts, processed_ids = 0, []

        for group in chunked(ids, IDS_GROUP_SIZE):
            try:
                data = fetch_group(group)  # ordenado por id desc
                rows_all: List[Dict[str, Any]] = []
                for t in data or []:
                    rows_all.extend(map_histories(t))

                # >>> insere com os maiores IDs primeiro (e, dentro do ticket, pelo changed_date)
                rows_all.sort(key=lambda r: (-int(r["ticket_id"]), str(r["changed_date"]) if r["changed_date"] else ""))

                if rows_all:
                    total_upserts += upsert_rows(conn, rows_all)

                returned = []
                for t in data or []:
                    try: returned.append(int(t.get("id")))
                    except Exception: pass
                if returned:
                    processed_ids.extend(returned)
            except requests.HTTPError as e:
                logging.warning("Falha no grupo %s: %s", group, e)

        if processed_ids:
            clear_audit(conn, list(set(processed_ids)))
        heartbeat(conn)
        logging.info("Finalizado. Upserts: %d | IDs limpos: %d",
                     total_upserts, len(set(processed_ids)))

if __name__ == "__main__":
    main()
