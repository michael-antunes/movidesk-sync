#!/usr/bin/env python3
import os, time, json, math, requests, psycopg2, psycopg2.extras
from datetime import datetime, timezone

API_BASE = "https://api.movidesk.com/public/v1/tickets"

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

# ---------- DB helpers ----------

def get_conn():
    return psycopg2.connect(NEON_DSN)

def select_next_ticket_ids(conn, limit:int):
    """
    Pega os próximos IDs de tickets a processar a partir da visão de resolvidos,
    ignorando o que já está atualizado em resolvidos_acoes.
    """
    sql = """
    with base as (
        select t.ticket_id, coalesce(t.last_update, t.last_resolved_at, t.last_closed_at) as ref_dt
        from visualizacao_resolvidos.tickets_resolvidos t
    )
    select b.ticket_id
    from base b
    left join visualizacao_resolvidos.resolvidos_acoes ra
      on ra.ticket_id = b.ticket_id
    where ra.ticket_id is null
       or ra.updated_at is null
       or ra.updated_at < b.ref_dt
    order by b.ref_dt desc nulls last, b.ticket_id desc
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [r[0] for r in cur.fetchall()]

def upsert_actions(conn, rows):
    """
    rows: iterable de (ticket_id, acoes_json, qtd_descricoes)
    """
    sql = """
    insert into visualizacao_resolvidos.resolvidos_acoes
        (ticket_id, acoes, qtd_acoes_descricao_plain, qtd_acoes_descricao_html, updated_at)
    values (%s, %s::jsonb, %s, 0, now())
    on conflict (ticket_id) do update set
        acoes = excluded.acoes,
        qtd_acoes_descricao_plain = excluded.qtd_acoes_descricao_plain,
        updated_at = excluded.updated_at
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()

# ---------- API helpers ----------

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
    """
    Chama /tickets com manejo de 429/5xx e do header retry-after.
    """
    params = dict(params or {})
    params["token"] = API_TOKEN

    last_err_txt = None
    for attempt in range(max_retries):
        r = SESS.get(API_BASE, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        # backoff 429/5xx
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_from_retry_after(r):
                continue
            time.sleep(1 + attempt * 2)
            continue
        # outros erros: propaga
        try:
            last_err_txt = r.text
        except Exception:
            last_err_txt = None
        r.raise_for_status()

    # estourou tentativas
    raise requests.HTTPError(f"{r.status_code} {r.reason} - body: {last_err_txt}", response=r)

def build_expand_param():
    """
    Monta o $expand para actions com os campos corretos e expansões aninhadas.
    OData do Movidesk permite repetir 'actions(...)' para $select e $expand.
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
    # combinamos os dois, separados por vírgula
    return f"{a_select},{a_expand}"

def fetch_actions_for_ids(ticket_ids):
    """
    Busca as ações no /tickets via $expand=actions(...) para um conjunto de IDs.
    Retorna { ticket_id: [actions...] } (só com os campos requeridos).
    """
    if not ticket_ids:
        return {}

    # filtro 'id eq X or id eq Y ...'
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
        # Garante somente os campos solicitados
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
    return out

# ---------- main ----------

def main():
    with get_conn() as conn:
        ids = select_next_ticket_ids(conn, BATCH_SIZE)
        if not ids:
            print("Nenhum ticket pendente para ações.")
            return

        print(f"Processando {len(ids)} ticket(s): {ids}")

        actions_map = fetch_actions_for_ids(ids)

        rows = []
        for tid in ids:
            acoes = actions_map.get(tid, [])
            # contador simples de descrições em texto
            qtd_plain = sum(1 for a in acoes if (a.get("description") or "").strip())
            rows.append((tid, json.dumps(acoes, ensure_ascii=False), qtd_plain))

        upsert_actions(conn, rows)
        print(f"Gravado/atualizado ações para {len(rows)} ticket(s).")

if __name__ == "__main__":
    main()
