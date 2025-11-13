#!/usr/bin/env python3
import os, time, json, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

# ======= CONTROLES =======
# Se BATCH_SIZE <= 0: roda "ilimitado" (consome tudo que estiver na audit em laços)
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "0"))
# Quantos IDs da audit por passada (por segurança/memória)
AUDIT_LIMIT   = int(os.getenv("AUDIT_LIMIT", "300"))
# Tamanho do pedaço por chamada ao Movidesk (reduz dinamicamente se 400)
ACTIONS_CHUNK = int(os.getenv("ACTIONS_CHUNK", "10"))
THROTTLE_SEC  = float(os.getenv("THROTTLE_SEC", "0.5"))
# Se quiser completar com pendências do tickets_resolvidos quando a audit zerar
FILL_FROM_TICKETS = os.getenv("FILL_FROM_TICKETS", "false").lower() in ("1","true","yes")
# =========================

SCHEMA    = "visualizacao_resolvidos"
T_TICKETS = f"{SCHEMA}.tickets_resolvidos"
T_ACOES   = f"{SCHEMA}.resolvidos_acoes"
T_AUDIT   = f"{SCHEMA}.audit_recent_missing"

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/acoes"})

def get_conn():
    return psycopg2.connect(NEON_DSN)

def cleanup_incomplete_rows(conn):
    sql = f"""
    delete from {T_ACOES} r
    where r.acoes is null
       or jsonb_typeof(r.acoes) <> 'array'
       or jsonb_array_length(r.acoes) = 0
       or not exists (select 1 from jsonb_array_elements(r.acoes) a(el) where a.el ? 'id')
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def get_audit_ids(conn, limit_):
    sql = f"""
      select ticket_id
      from {T_AUDIT}
      where table_name = 'resolvidos_acoes'
      order by run_id desc, ticket_id desc
      limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall()
    return [r[0] for r in rows]

def clear_audit_ids(conn, ids):
    if not ids:
        return
    sql = f"""
      delete from {T_AUDIT}
      where table_name = 'resolvidos_acoes'
        and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()

def select_next_ticket_ids(conn, limit_, exclude_ids):
    sql = f"""
    with base as (
        select t.ticket_id, coalesce(t.last_update, t.last_resolved_at, t.last_closed_at) as ref_dt
        from {T_TICKETS} t
    )
    select b.ticket_id
    from base b
    left join {T_ACOES} ra on ra.ticket_id = b.ticket_id
    where (ra.ticket_id is null or ra.updated_at is null or (b.ref_dt is not null and ra.updated_at < b.ref_dt))
      and not (b.ticket_id = any(%s::int[]))
    order by b.ref_dt desc nulls last, b.ticket_id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (exclude_ids or [], limit_))
        return [r[0] for r in cur.fetchall()]

def _sleep_retry_after(r):
    try:
        ra = r.headers.get("retry-after")
        if not ra: return False
        time.sleep(max(1, int(str(ra).strip())))
        return True
    except Exception:
        return False

def movidesk_request(params, max_retries=4):
    p = dict(params or {})
    p["token"] = API_TOKEN
    for i in range(max_retries):
        r = SESS.get(API_BASE, params=p, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429,500,502,503,504):
            if _sleep_retry_after(r):
                continue
            time.sleep(1 + 2*i)
            continue
        r.raise_for_status()
    r.raise_for_status()

def build_expand_param():
    a1 = "actions($select=id,type,origin,description,status,justification,createdDate,createdBy,isDeleted,tags)"
    a2 = "actions($expand=timeAppointments($select=id,activity,date,periodStart,periodEnd,workTime,accountedTime,workTypeName,createdBy,createdByTeam),attachments($select=fileName,path,createdBy,createdDate))"
    return f"{a1},{a2}"

def fetch_actions_chunk(ids_chunk):
    if not ids_chunk:
        return []
    filtro = " or ".join([f"id eq {int(i)}" for i in ids_chunk])
    params = {
        "$select": "id,lastUpdate",
        "$filter": filtro,
        "$expand": build_expand_param(),
        "$top": 100,
        "includeDeletedItems": "true",
    }
    return movidesk_request(params) or []

def fetch_actions_for_ids(ids):
    out = {}
    ids = list(dict.fromkeys(ids))
    chunk = max(1, ACTIONS_CHUNK)
    i = 0
    while i < len(ids):
        part = ids[i:i+chunk]
        try:
            data = fetch_actions_chunk(part)
            for item in data:
                tid = int(item.get("id"))
                actions = item.get("actions") or []
                cleaned = []
                for a in actions:
                    cleaned.append({
                        "id": a.get("id"),
                        "type": a.get("type"),
                        "origin": a.get("origin"),
                        "description": a.get("description"),
                        "status": a.get("status"),
                        "justification": a.get("justification"),
                        "createdDate": a.get("createdDate"),
                        "createdBy": a.get("createdBy"),
                        "isDeleted": a.get("isDeleted"),
                        "timeAppointments": a.get("timeAppointments"),
                        "attachments": a.get("attachments"),
                        "tags": a.get("tags"),
                    })
                out[tid] = cleaned
            i += len(part)
            time.sleep(THROTTLE_SEC)
        except requests.HTTPError as e:
            if chunk == 1:
                raise
            chunk = max(1, chunk // 2)
    return out

def upsert_actions(conn, rows):
    sql = f"""
    insert into {T_ACOES} (ticket_id, acoes, updated_at)
    values (%s, %s::jsonb, now())
    on conflict (ticket_id) do update set
      acoes = excluded.acoes,
      updated_at = excluded.updated_at
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

def process_ids(conn, ids):
    if not ids:
        return 0, 0
    print(f"Processando {len(ids)} ticket(s): {ids[:10]}{' ...' if len(ids)>10 else ''}")
    actions_map = fetch_actions_for_ids(ids)
    rows, processed = [], []
    for tid in ids:
        acoes = actions_map.get(tid, [])
        rows.append((tid, json.dumps(acoes, ensure_ascii=False)))
        if tid in actions_map:  # limpa audit só quando veio payload do Movidesk
            processed.append(tid)
    if rows:
        upsert_actions(conn, rows)
    if processed:
        clear_audit_ids(conn, processed)
    return len(rows), len(processed)

def main():
    total_rows, total_cleared = 0, 0
    with get_conn() as conn:
        cleanup_incomplete_rows(conn)

        ilimitado = (BATCH_SIZE <= 0)

        while True:
            # 1) pega um "page" da audit
            page_limit = AUDIT_LIMIT if ilimitado else min(AUDIT_LIMIT, BATCH_SIZE)
            ids = get_audit_ids(conn, page_limit)

            # 2) se não tem audit…
            if not ids:
                if not FILL_FROM_TICKETS:
                    break
                # …opcionalmente completa do tickets_resolvidos
                ids = select_next_ticket_ids(conn, page_limit, [])
                if not ids:
                    break

            # 3) em modo limitado, corta para BATCH_SIZE
            if not ilimitado and len(ids) > BATCH_SIZE:
                ids = ids[:BATCH_SIZE]

            got, cleared = process_ids(conn, ids)
            total_rows += got
            total_cleared += cleared

            # 4) sai se era um único lote
            if not ilimitado:
                break

            # 5) em modo ilimitado, continua até a audit esvaziar
            if got == 0:
                break

        print(f"Gravou {total_rows} ticket(s) e limpou {total_cleared} da audit.")

if __name__ == "__main__":
    main()
