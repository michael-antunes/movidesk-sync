#!/usr/bin/env python3
import os, time, json, requests, psycopg2, psycopg2.extras

API_BASE   = "https://api.movidesk.com/public/v1/tickets"
API_TOKEN  = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN   = os.getenv("NEON_DSN")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

# ---------------------- DB ----------------------

def get_conn():
    return psycopg2.connect(NEON_DSN)

def cleanup_incomplete_rows(conn):
    """
    Remove registros com JSON de ações incompleto, para forçar reprocessamento.
    Critérios:
      - acoes IS NULL
      - acoes não é array
      - acoes é array vazio
      - nenhum item do array possui 'id'
    """
    sql = """
    delete from visualizacao_resolvidos.resolvidos_acoes r
    where
        r.acoes is null
        or jsonb_typeof(r.acoes) <> 'array'
        or jsonb_array_length(r.acoes) = 0
        or not exists (
            select 1
            from jsonb_array_elements(r.acoes) as a(el)
            where (a.el ? 'id')  -- ao menos um item com chave 'id'
        )
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        deleted = cur.rowcount
    conn.commit()
    if deleted:
        print(f"Limpeza: apagadas {deleted} linhas com JSON incompleto em resolvidos_acoes.")

def select_next_ticket_ids(conn, limit:int):
    """
    Pega até 'limit' tickets de tickets_resolvidos que:
      - ainda não têm linha em resolvidos_acoes, OU
      - têm linha, mas updated_at < last_update/last_resolved_at/last_closed_at
    """
    sql = """
    with base as (
        select
            t.ticket_id,
            coalesce(t.last_update, t.last_resolved_at, t.last_closed_at) as ref_dt
        from visualizacao_resolvidos.tickets_resolvidos t
    )
    select b.ticket_id
    from base b
    left join visualizacao_resolvidos.resolvidos_acoes ra
      on ra.ticket_id = b.ticket_id
    where ra.ticket_id is null
       or ra.updated_at is null
       or (b.ref_dt is not null and ra.updated_at < b.ref_dt)
    order by b.ref_dt desc nulls last, b.ticket_id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [r[0] for r in cur.fetchall()]

def upsert_actions(conn, rows):
    """
    rows: [(ticket_id, acoes_json_str), ...]
    OBS: gravamos somente ticket_id, acoes e updated_at.
    """
    sql = """
    insert into visualizacao_resolvidos.resolvidos_acoes
        (ticket_id, acoes, updated_at)
    values (%s, %s::jsonb, now())
    on conflict (ticket_id) do update set
        acoes = excluded.acoes,
        updated_at = excluded.updated_at
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

# ---------------------- API ----------------------

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/acoes"})

def _sleep_from_retry_after(r):
    try:
        ra = r.headers.get("retry-after")
        if ra is None:
            return False
        secs = int(str(ra).strip())
        time.sleep(max(1, secs))
        return True
    except Exception:
        return False

def movidesk_request(params, max_retries=4):
    params = dict(params or {})
    params["token"] = API_TOKEN

    for attempt in range(max_retries):
        r = SESS.get(API_BASE, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_from_retry_after(r):
                continue
            time.sleep(1 + attempt * 2)
            continue
        # outros erros: propaga com body
        body = None
        try:
            body = r.text
        except Exception:
            pass
        raise requests.HTTPError(f"{r.status_code} {r.reason} - body: {body}", response=r)

    raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)

def build_expand_param():
    """
    Campos solicitados (sem HTML, conforme a API de /tickets):
    id, type, origin, description, status, justification, createdDate, createdBy, isDeleted, timeAppointments, attachments, tags
    """
    a_select = (
        "actions($select="
        "id,type,origin,description,status,justification,createdDate,createdBy,isDeleted,tags)"
    )
    a_expand = (
        "actions($expand="
        "timeAppointments($select=id,activity,date,periodStart,periodEnd,workTime,accountedTime,workTypeName,createdBy,createdByTeam),"
        "attachments($select=fileName,path,createdBy,createdDate))"
    )
    return f"{a_select},{a_expand}"

def fetch_actions_for_ids(ticket_ids):
    if not ticket_ids:
        return {}

    id_filter = " or ".join([f"id eq {int(i)}" for i in ticket_ids])

    params = {
        "$select": "id,lastUpdate",
        "$filter": id_filter,
        "$expand": build_expand_param(),
        "$top": 100,
        "includeDeletedItems": "true",
    }
    data = movidesk_request(params) or []
    out = {}
    for item in data:
        tid = int(item.get("id"))
        actions = item.get("actions") or []
        cleaned = []
        for a in actions:
            cleaned.append({
                "id":            a.get("id"),
                "type":          a.get("type"),
                "origin":        a.get("origin"),
                "description":   a.get("description"),
                "status":        a.get("status"),
                "justification": a.get("justification"),
                "createdDate":   a.get("createdDate"),
                "createdBy":     a.get("createdBy"),
                "isDeleted":     a.get("isDeleted"),
                "timeAppointments": a.get("timeAppointments"),
                "attachments":     a.get("attachments"),
                "tags":            a.get("tags"),
            })
        out[tid] = cleaned
    return out

# ---------------------- main ----------------------

def main():
    with get_conn() as conn:
        # 1) limpa linhas incompletas (força reprocessar)
        cleanup_incomplete_rows(conn)

        # 2) escolhe próximos tickets
        ids = select_next_ticket_ids(conn, BATCH_SIZE)
        if not ids:
            print("Nenhum ticket pendente para ações.")
            return

        print(f"Processando {len(ids)} ticket(s): {ids}")

        # 3) coleta na API
        actions_map = fetch_actions_for_ids(ids)

        # 4) grava
        rows = []
        for tid in ids:
            acoes = actions_map.get(tid, [])
            rows.append((tid, json.dumps(acoes, ensure_ascii=False)))
        upsert_actions(conn, rows)

        print(f"Gravado/atualizado ações para {len(rows)} ticket(s).")

if __name__ == "__main__":
    main()
