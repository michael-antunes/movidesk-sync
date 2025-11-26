#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, time, json, logging, requests, psycopg2
from datetime import datetime, timezone
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

API_BASE = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

SCHEMA_AUD = "visualizacao_resolvidos"
SCHEMA_DST = "visualizacao_resolucao"
T_AUDIT = f"{SCHEMA_AUD}.audit_recent_missing"
T_RPS   = f"{SCHEMA_DST}.resolucao_por_status"

IDS_GROUP_SIZE = int(os.getenv("IDS_GROUP_SIZE", "12"))
AUDIT_LIMIT    = int(os.getenv("AUDIT_LIMIT", "300"))
THROTTLE_SEC   = float(os.getenv("THROTTLE_SEC", "0.35"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/resolucao-status"})


def _sleep_retry_after(r):
    try:
        ra = r.headers.get("retry-after")
        if ra:
            time.sleep(max(1, int(str(ra).strip())))
            return True
    except Exception:
        pass
    return False


def md_get(params, max_retries=4):
    p = dict(params or {})
    p["token"] = API_TOKEN
    last_err = None
    for i in range(max_retries):
        r = SESS.get(API_BASE, params=p, timeout=60)
        if r.status_code == 200:
            try:
                js = r.json() or []
            except ValueError:
                return []
            return js
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_retry_after(r):
                continue
            time.sleep(1 + 2 * i)
            last_err = requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
            continue
        r.raise_for_status()
    if last_err:
        last_err.response.raise_for_status()
    return []


def get_audit_ids(conn, limit_):
    sql = f"""
      select ticket_id
        from {T_AUDIT}
       where table_name = 'resolucao_por_status'
       order by run_id desc, ticket_id desc
       limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall() or []
    ids = [r[0] for r in rows]
    logging.info("Lote da fila (maiores IDs primeiro): %s%s",
                 ids[:10], " ..." if len(ids) > 10 else "")
    return ids


def clear_audit_ids(conn, ids):
    if not ids:
        return
    sql = f"""
      delete from {T_AUDIT}
       where table_name = 'resolucao_por_status'
         and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()


def build_expand_param():
    exp = "statusHistories($select=status,justification,permanencyTimeFullTime,permanencyTimeWorkingTime,changedDate;$expand=changedBy($select=id,businessName))"
    logging.info("EXPAND usado (debug): %s", exp)
    return exp


def fetch_chunk(ids_chunk):
    if not ids_chunk:
        return []
    filtro = " or ".join([f"id eq {int(i)}" for i in ids_chunk])
    params = {
        "$select": "id,ownerTeam",
        "$expand": build_expand_param(),
        "$filter": filtro,
        "$top": 100,
        "includeDeletedItems": "true",
    }
    raw = md_get(params) or []
    if isinstance(raw, dict):
        data = raw.get("items") or raw.get("value") or []
    else:
        data = raw
    if isinstance(data, dict):
        data = data.get("items") or data.get("value") or []
    data = data or []
    logging.info("fetch_chunk: ids=%s... -> %d item(s) da API",
                 ids_chunk[:5], len(data))
    return data


def normalize_changed_date(s: str) -> str | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0, tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s


def map_rows(item):
    out = []
    tid = int(item.get("id"))
    owner_team = item.get("ownerTeam") or ""
    if isinstance(owner_team, dict):
        owner_team = owner_team.get("businessName") or ""
    for h in (item.get("statusHistories") or []):
        status = h.get("status") or ""
        justific = h.get("justification") or ""
        sec_work = h.get("permanencyTimeWorkingTime") or 0
        sec_full = h.get("permanencyTimeFullTime") or 0
        chg = h.get("changedBy") or {}
        agent = chg.get("businessName") if isinstance(chg, dict) else ""
        dt = normalize_changed_date(h.get("changedDate"))
        row = (
            tid,
            status,
            justific,
            int(sec_work or 0),
            float(sec_full or 0.0),
            json.dumps(chg, ensure_ascii=False),
            dt,
            agent or "",
            owner_team or "",
            ""
        )
        out.append(row)
    return out


UPSERT_SQL = f"""
insert into {T_RPS}
(ticket_id, status, justificativa, seconds_uti, permanency_time_fulltime_seconds,
 changed_by, changed_date, agent_name, team_name, time_squad)
values %s
on conflict (ticket_id, status, justificativa, changed_date) do update set
  seconds_uti = excluded.seconds_uti,
  permanency_time_fulltime_seconds = excluded.permanency_time_fulltime_seconds,
  changed_by  = excluded.changed_by,
  agent_name  = excluded.agent_name,
  team_name   = excluded.team_name,
  time_squad  = excluded.time_squad,
  imported_at = now()
"""


def dedupe_rows(rows):
    best = {}
    for r in rows:
        key = (r[0], r[1], r[2], r[6])
        cur = best.get(key)
        if cur is None:
            best[key] = r
            continue
        score_cur = (1 if (cur[7] or cur[8]) else 0, cur[3])
        score_new = (1 if (r[7] or r[8]) else 0, r[3])
        if score_new > score_cur:
            best[key] = r
    return list(best.values())


def upsert_rows(conn, rows):
    if not rows:
        return 0
    rows = dedupe_rows(rows)
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def main():
    total_upserts = 0
    cleared = 0

    with psycopg2.connect(NEON_DSN) as conn:
        ids = get_audit_ids(conn, AUDIT_LIMIT)
        if not ids:
            logging.info("Fila vazia para 'resolucao_por_status'. Nada a fazer.")
            return

        all_rows = []
        processed_ids = set()
        i = 0
        while i < len(ids):
            chunk = ids[i:i + IDS_GROUP_SIZE]
            i += IDS_GROUP_SIZE
            try:
                data = fetch_chunk(chunk)
            except requests.HTTPError as e:
                logging.warning("Erro HTTP duro em fetch_chunk: %s :: %s", e, getattr(e.response, "text", ""))
                continue

            for item in data or []:
                try:
                    tid = int(item.get("id"))
                except Exception:
                    continue
                rows = map_rows(item)
                if rows:
                    all_rows.extend(rows)
                    processed_ids.add(tid)

            time.sleep(THROTTLE_SEC)

        wrote = upsert_rows(conn, all_rows)
        total_upserts += wrote
        logging.info("Gravadas %d linha(s) em %s.", wrote, T_RPS)

        if processed_ids:
            clear_audit_ids(conn, list(processed_ids))
            cleared += len(processed_ids)
            logging.info("Limpou %d ticket(s) da audit.", len(processed_ids))
        else:
            logging.info("Nenhum ticket com payload; audit não foi limpa.")

    logging.info("Fim. upserts=%d, audit_cleared_ids=%d", total_upserts, cleared)


if __name__ == "__main__":
    main()
