import os, time, requests, psycopg2, psycopg2.extras, datetime

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
FALLBACK_PERSON = os.getenv("MOVIDESK_FALLBACK_PERSON_ORG", "0") == "1"

http = requests.Session()
http.headers.update({"Accept":"application/json"})

def _req(url, params, timeout=90):
    while True:
        r = http.get(url, params=params, timeout=timeout)
        if r.status_code in (429,503):
            wait = int(r.headers.get("retry-after") or 60); time.sleep(wait); continue
        if r.status_code == 404: return {}
        r.raise_for_status(); return r.json() if r.text else {}

def to_bool(v):
    s=str(v).strip().lower()
    return s in ("true","1","sim","yes","y")

def to_ts(v):
    try:
        if not v: return None
        return datetime.datetime.fromisoformat(str(v).replace("Z","+00:00"))
    except: return None

def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except: return None

def get_person_org(person_id):
    if not FALLBACK_PERSON or not person_id:
        return None, None, None
    data = _req(f"{API_BASE}/persons", {
        "token": API_TOKEN,
        "$select": "id",
        "$filter": f"id eq '{person_id}'",
        "$expand": "organizations"
    }) or []
    if not data or not isinstance(data, list):
        return None, None, None
    orgs = (data[0] or {}).get("organizations") or []
    if not orgs:
        return None, None, None
    o = orgs[0] or {}
    return o.get("id"), o.get("businessName"), o.get("codeReferenceAdditional")

def get_cf(cfs, cid):
    if not isinstance(cfs, list): return None
    for f in cfs:
        if str(f.get("id")) == cid: return f.get("value")
    return None

def count_public_actions(actions):
    if not isinstance(actions, list): return 0
    n=0
    for a in actions:
        if a is None: continue
        if a.get("isPublic") is True: n+=1; continue
        t=str(a.get("type") or "").lower()
        v=str(a.get("visibility") or "").lower()
        if t in ("public","publicreply","publicnote","email","message") or v=="public": n+=1
    return n

def transitions(actions):
    r=None; c=None
    if isinstance(actions, list):
        for a in actions:
            dt=a.get("createdDate") or a.get("date")
            bs=str(a.get("baseStatus") or a.get("newBaseStatus") or a.get("status") or "").lower()
            if dt:
                if ("resolved" in bs) or ("resolvido" in bs):
                    if r is None: r=dt
                if ("closed" in bs) or ("fechado" in bs):
                    if c is None: c=dt
            chs=a.get("changes") or []
            for ch in chs:
                f=str(ch.get("field") or "").lower()
                nv=str(ch.get("newValue") or "").lower()
                cd=ch.get("date") or dt
                if f in ("basestatus","status"):
                    if ("resolved" in nv) or ("resolvido" in nv):
                        if r is None: r=cd
                    if ("closed" in nv) or ("fechado" in nv):
                        if c is None: c=cd
    return r,c

CF = {
    "csat":"137641",
    "aberto_via":"184387",
    "work_item":"215636",
    "problema_generalizado":"129782",
    "plantao":"111727",
    "aud_comentario":"96132",
    "aud_data":"99086",
    "aud_solucao_aprov":"98922",
    "mesclado":"141736",
    "primeiro_resp":"227413",
}

UPSERT_DETAIL_CTRL = """
insert into visualizacao_resolvidos.detail_control (ticket_id,resolved_at,closed_at,last_update)
values (%(id)s,%(resolved_at)s,%(closed_at)s,%(last_update)s)
on conflict (ticket_id) do update set
  resolved_at = coalesce(excluded.resolved_at, visualizacao_resolvidos.detail_control.resolved_at),
  closed_at   = coalesce(excluded.closed_at,   visualizacao_resolvidos.detail_control.closed_at),
  last_update = excluded.last_update
"""

UPSERT_TICKETS = """
insert into visualizacao_resolvidos.tickets_resolvidos
(id,status,responsible_id,responsible_name,service_first_level,service_second_level,service_third_level,
 cf_137641_avaliado_csat,cf_184387_aberto_via,organization_id,organization_name,public_actions_count,category,
 cf_215636_id_work_item,cf_129782_problema_generalizado,cf_111727_atendimento_plantao,cf_96132_aud_comentario,
 cf_99086_aud_data_auditoria,cf_98922_aud_solucao_aprovada,urgency,cf_141736_mesclado,cf_227413_primeiro_responsavel)
values
(%(id)s,%(status)s,%(responsible_id)s,%(responsible_name)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(cf_137641_avaliado_csat)s,%(cf_184387_aberto_via)s,%(organization_id)s,%(organization_name)s,%(public_actions_count)s,%(category)s,
 %(cf_215636_id_work_item)s,%(cf_129782_problema_generalizado)s,%(cf_111727_atendimento_plantao)s,%(cf_96132_aud_comentario)s,
 %(cf_99086_aud_data_auditoria)s,%(cf_98922_aud_solucao_aprovada)s,%(urgency)s,%(cf_141736_mesclado)s,%(cf_227413_primeiro_responsavel)s)
on conflict (id) do update set
  status = excluded.status,
  responsible_id = excluded.responsible_id,
  responsible_name = excluded.responsible_name,
  service_first_level = excluded.service_first_level,
  service_second_level = excluded.service_second_level,
  service_third_level = excluded.service_third_level,
  cf_137641_avaliado_csat = excluded.cf_137641_avaliado_csat,
  cf_184387_aberto_via = excluded.cf_184387_aberto_via,
  organization_id = excluded.organization_id,
  organization_name = excluded.organization_name,
  public_actions_count = excluded.public_actions_count,
  category = excluded.category,
  cf_215636_id_work_item = excluded.cf_215636_id_work_item,
  cf_129782_problema_generalizado = excluded.cf_129782_problema_generalizado,
  cf_111727_atendimento_plantao = excluded.cf_111727_atendimento_plantao,
  cf_96132_aud_comentario = excluded.cf_96132_aud_comentario,
  cf_99086_aud_data_auditoria = excluded.cf_99086_aud_data_auditoria,
  cf_98922_aud_solucao_aprovada = excluded.cf_98922_aud_solucao_aprovada,
  urgency = excluded.urgency,
  cf_141736_mesclado = excluded.cf_141736_mesclado,
  cf_227413_primeiro_responsavel = excluded.cf_227413_primeiro_responsavel
"""

def pending_ids(conn, limit=400):
    overlap_min = int(os.getenv("DETAIL_OVERLAP_MIN","30"))
    with conn.cursor() as cur:
        cur.execute("select max(coalesce(last_detail_run_at,'epoch'::timestamptz)) from visualizacao_resolvidos.sync_control")
        since = cur.fetchone()[0] or datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
        since -= datetime.timedelta(minutes=overlap_min)
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.detail_control
             where last_update > %s
          order by last_update asc
             limit %s
        """, (since, limit))
        ids = [r[0] for r in cur.fetchall()]
    return ids

def fetch_detail(ticket_id):
    url = f"{API_BASE}/tickets/{ticket_id}"
    expand = ",".join([
        "owner",
        "clients($expand=organization)",
        "actions",
        "customFields"
    ])
    fields = ",".join([
        "id","status","lastUpdate","serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
        "urgency","owner/id","owner/businessName","clients/id","clients/businessName",
        "clients/organization/id","clients/organization/businessName"
    ])
    return _req(url, {"token":API_TOKEN, "$select":fields, "$expand":expand}) or {}

def map_row(t):
    owner = t.get("owner") or {}
    agent_id = iint(owner.get("id"))
    resp_name = owner.get("businessName")

    clients = t.get("clients") or []
    c0 = clients[0] if clients else {}
    first_client_name = c0.get("businessName")
    first_client_id = c0.get("id")

    org = c0.get("organization") or {}
    empresa_id = org.get("id")
    empresa_nome = org.get("businessName")

    if not empresa_id and FALLBACK_PERSON:
        pid = first_client_id or ((t.get("createdBy") or {}).get("id"))
        eid, enome, _ = get_person_org(pid)
        empresa_id, empresa_nome = eid, enome

    if not empresa_nome:
        empresa_nome = first_client_name

    actions = t.get("actions") or []
    cfields = t.get("customFields") or []
    resolved_at, closed_at = transitions(actions)

    return {
        "id": iint(t.get("id")),
        "status": t.get("status"),
        "responsible_id": agent_id,
        "responsible_name": resp_name,
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "cf_137641_avaliado_csat": to_bool(get_cf(cfields, CF["csat"])) if get_cf(cfields, CF["csat"]) is not None else None,
        "cf_184387_aberto_via": get_cf(cfields, CF["aberto_via"]),
        "organization_id": str(empresa_id) if empresa_id is not None else None,
        "organization_name": empresa_nome,
        "public_actions_count": count_public_actions(actions),
        "category": t.get("category"),
        "cf_215636_id_work_item": str(get_cf(cfields, CF["work_item"])) if get_cf(cfields, CF["work_item"]) is not None else None,
        "cf_129782_problema_generalizado": to_bool(get_cf(cfields, CF["problema_generalizado"])) if get_cf(cfields, CF["problema_generalizado"]) is not None else None,
        "cf_111727_atendimento_plantao": to_bool(get_cf(cfields, CF["plantao"])) if get_cf(cfields, CF["plantao"]) is not None else None,
        "cf_96132_aud_comentario": get_cf(cfields, CF["aud_comentario"]),
        "cf_99086_aud_data_auditoria": to_ts(get_cf(cfields, CF["aud_data"])),
        "cf_98922_aud_solucao_aprovada": to_bool(get_cf(cfields, CF["aud_solucao_aprov"])) if get_cf(cfields, CF["aud_solucao_aprov"]) is not None else None,
        "urgency": iint(t.get("urgency")),
        "cf_141736_mesclado": to_bool(get_cf(cfields, CF["mesclado"])) if get_cf(cfields, CF["mesclado"]) is not None else None,
        "cf_227413_primeiro_responsavel": get_cf(cfields, CF["primeiro_resp"]),
        "resolved_at": to_ts(resolved_at),
        "closed_at": to_ts(closed_at),
        "last_update": t.get("lastUpdate"),
    }

def main():
    if not API_TOKEN or not NEON_DSN: raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    conn = psycopg2.connect(NEON_DSN)
    with conn.cursor() as cur: cur.execute("set time zone 'UTC'")
    conn.commit()
    throttle = float(os.getenv("MOVIDESK_THROTTLE","0.25"))
    batch = int(os.getenv("DETAIL_BATCH","200"))
    force_ids = [iint(x) for x in os.getenv("DETAIL_FORCE_IDS","").split(",") if str(x).strip().isdigit()]
    try:
        ids = pending_ids(conn, batch)
        ids = list(dict.fromkeys(force_ids + ids))
        rows=[]
        for tid in ids:
            t = fetch_detail(tid)
            if not isinstance(t, dict) or not t: continue
            r = map_row(t)
            if r.get("id") is None: continue
            rows.append(r)
            time.sleep(throttle)
        if rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_DETAIL_CTRL, rows, page_size=200)
                psycopg2.extras.execute_batch(cur, UPSERT_TICKETS, rows, page_size=200)
            conn.commit()
            max_processed = max([to_ts(r["last_update"]) for r in rows if r.get("last_update")], default=None)
            if max_processed:
                with conn.cursor() as cur:
                    cur.execute("""
                        update visualizacao_resolvidos.sync_control
                           set last_detail_run_at = GREATEST(coalesce(last_detail_run_at,'epoch'::timestamptz), %s)
                    """, (max_processed,))
                    if cur.rowcount == 0:
                        cur.execute("insert into visualizacao_resolvidos.sync_control (last_detail_run_at) values (%s)", (max_processed,))
                conn.commit()
        print(f"DETAIL processed={len(rows)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
