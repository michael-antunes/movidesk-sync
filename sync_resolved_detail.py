import os, time, requests, psycopg2, psycopg2.extras, datetime

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
OPENING_METHOD_FIELD_IDS = [x.strip() for x in os.getenv("MOVIDESK_OPENING_METHOD_FIELD_IDS","").split(",") if x.strip()]

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            wait = int(r.headers.get("retry-after") or 60); time.sleep(wait); continue
        if r.status_code == 404: return {}
        r.raise_for_status(); return r.json() if r.text else {}

def pick_opening_method(custom_fields):
    if not isinstance(custom_fields, list): return None
    for f in custom_fields:
        fid = str(f.get("id") or "")
        label = str(f.get("label") or "")
        val = f.get("value")
        if OPENING_METHOD_FIELD_IDS and fid in OPENING_METHOD_FIELD_IDS:
            return val if isinstance(val, str) else (str(val) if val is not None else None)
        if "forma de abertura" in label.lower():
            return val if isinstance(val, str) else (str(val) if val is not None else None)
    return None

def count_public_actions(actions):
    if not isinstance(actions, list): return 0
    n = 0
    for a in actions:
        if a is None: continue
        if a.get("isPublic") is True: n += 1; continue
        t = str(a.get("type") or "").lower()
        v = str(a.get("visibility") or "").lower()
        if t in ("public","publicreply","publicnote","email","message") or v == "public": n += 1
    return n

def status_transition_dates(actions):
    resolved_at = None; closed_at = None
    if isinstance(actions, list):
        for a in actions:
            bs = str(a.get("baseStatus") or a.get("newBaseStatus") or "").lower()
            dt = a.get("createdDate") or a.get("date") or a.get("created") or None
            if not dt: continue
            if "resolved" in bs and resolved_at is None: resolved_at = dt
            if "closed" in bs and closed_at is None: closed_at = dt
    return resolved_at, closed_at

def map_row(t):
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if clients else {}
    org = c0.get("organization") or {}
    actions = t.get("actions") or []
    cfields = t.get("customFields") or []
    resolved_at, closed_at = status_transition_dates(actions)
    opening_method = pick_opening_method(cfields)
    return {
        "id": int(t.get("id")) if str(t.get("id")).isdigit() else None,
        "protocol": t.get("protocol"),
        "subject": t.get("subject"),
        "type": t.get("type"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "owner_team": t.get("ownerTeam"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "created_date": t.get("createdDate"),
        "last_update": t.get("lastUpdate"),
        "resolved_at": resolved_at,
        "closed_at": closed_at,
        "responsavel": owner.get("businessName"),
        "empresa_cod_ref_adicional": org.get("codeReferenceAdditional"),
        "agent_id": int(owner.get("id")) if str(owner.get("id") or "").isdigit() else None,
        "empresa_id": str(org.get("id")) if org.get("id") is not None else None,
        "empresa_nome": org.get("businessName") or c0.get("businessName"),
        "csat_avaliado": None,
        "public_actions_count": count_public_actions(actions),
        "opening_method": opening_method,
        "raw_owner": psycopg2.extras.Json(owner),
        "raw_clients": psycopg2.extras.Json(clients),
        "raw_actions": psycopg2.extras.Json(actions),
        "raw_custom_fields": psycopg2.extras.Json(cfields),
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(id,protocol,subject,type,status,base_status,owner_team,service_first_level,service_second_level,service_third_level,
 created_date,last_update,resolved_at,closed_at,responsavel,empresa_cod_ref_adicional,agent_id,empresa_id,empresa_nome,
 csat_avaliado,public_actions_count,opening_method,raw_owner,raw_clients,raw_actions,raw_custom_fields)
values
(%(id)s,%(protocol)s,%(subject)s,%(type)s,%(status)s,%(base_status)s,%(owner_team)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(created_date)s,%(last_update)s,%(resolved_at)s,%(closed_at)s,%(responsavel)s,%(empresa_cod_ref_adicional)s,%(agent_id)s,%(empresa_id)s,%(empresa_nome)s,
 %(csat_avaliado)s,%(public_actions_count)s,%(opening_method)s,%(raw_owner)s,%(raw_clients)s,%(raw_actions)s,%(raw_custom_fields)s)
on conflict (id) do update set
  protocol = excluded.protocol,
  subject = excluded.subject,
  type = excluded.type,
  status = excluded.status,
  base_status = excluded.base_status,
  owner_team = excluded.owner_team,
  service_first_level = excluded.service_first_level,
  service_second_level = excluded.service_second_level,
  service_third_level = excluded.service_third_level,
  created_date = excluded.created_date,
  last_update = excluded.last_update,
  resolved_at = coalesce(excluded.resolved_at, visualizacao_resolvidos.tickets_resolvidos.resolved_at),
  closed_at = coalesce(excluded.closed_at, visualizacao_resolvidos.tickets_resolvidos.closed_at),
  responsavel = excluded.responsavel,
  empresa_cod_ref_adicional = excluded.empresa_cod_ref_adicional,
  agent_id = excluded.agent_id,
  empresa_id = excluded.empresa_id,
  empresa_nome = excluded.empresa_nome,
  public_actions_count = excluded.public_actions_count,
  opening_method = excluded.opening_method,
  raw_owner = excluded.raw_owner,
  raw_clients = excluded.raw_clients,
  raw_actions = excluded.raw_actions,
  raw_custom_fields = excluded.raw_custom_fields;
"""

def update_detail_control(conn, rows):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,
            "insert into visualizacao_resolvidos.detail_control (ticket_id,last_update,resolved_at,closed_at,synced_at) values (%s,%s,%s,%s,now()) on conflict (ticket_id) do update set last_update=excluded.last_update,resolved_at=excluded.resolved_at,closed_at=excluded.closed_at,synced_at=now()",
            [(r["id"], r["last_update"], r["resolved_at"], r["closed_at"]) for r in rows], page_size=300)
    conn.commit()

def update_csat_flags(conn, ids):
    if not ids: return
    with conn.cursor() as cur:
        cur.execute("""
            update visualizacao_resolvidos.tickets_resolvidos t
            set csat_avaliado = exists (
                select 1
                from visualizacao_satisfacao.movidesk_pesquisa_satisfacao_respostas s
                where s.ticket_id = t.id
            )
            where t.id = any(%s)
        """, (ids,))
    conn.commit()

def get_pending_ids(conn, limit=400):
    with conn.cursor() as cur:
        cur.execute("""
            select i.id
            from visualizacao_resolvidos.tickets_resolvidos i
            left join visualizacao_resolvidos.detail_control d on d.ticket_id = i.id
            where d.ticket_id is null or i.last_update > coalesce(d.last_update, 'epoch'::timestamptz)
            order by i.last_update desc
            limit %s
        """, (limit,))
        return [r[0] for r in cur.fetchall()]

def fetch_detail(ticket_id):
    url = f"{API_BASE}/tickets/{ticket_id}"
    return _req(url, {
        "token": API_TOKEN,
        "$expand": "owner,clients($expand=organization),actions,customFields"
    }) or {}

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    conn = psycopg2.connect(NEON_DSN)
    try:
        ids = get_pending_ids(conn, int(os.getenv("DETAIL_BATCH","300")))
        rows = []
        for tid in ids:
            t = fetch_detail(tid)
            if not isinstance(t, dict) or not t: continue
            rows.append(map_row(t))
            time.sleep(float(os.getenv("MOVIDESK_THROTTLE","0.2")))
        if rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
            conn.commit()
            update_detail_control(conn, rows)
            update_csat_flags(conn, [r["id"] for r in rows])
        print(f"DETAIL UPSERT: {len(rows)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
