#!/usr/bin/env python3
import os, time, json, sys
import requests
import psycopg2
from psycopg2.extras import execute_batch

API_BASE = "https://api.movidesk.com/public/v1"

def env(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and not val:
        print(f"Missing env {name}", file=sys.stderr); sys.exit(1)
    return val

TOKEN   = env("MOVIDESK_TOKEN", required=True)
DSN     = env("NEON_DSN", required=True)
THROTTLE= float(env("MOVIDESK_THROTTLE", "0.25"))
RUN_ID_OVERRIDE = os.getenv("AUDIT_RUN_ID")  # opcional: processar um run específico

# ---------- Movidesk helpers ----------

def http_get(url):
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    return r.json()

def get_ticket_full(ticket_id):
    """Tenta baixar o ticket (com actions). Variações do endpoint para garantir compatibilidade."""
    candidates = [
        f"{API_BASE}/tickets/{ticket_id}?token={TOKEN}&$expand=actions",
        f"{API_BASE}/tickets/{ticket_id}?token={TOKEN}&include=actions",
        f"{API_BASE}/tickets/{ticket_id}?token={TOKEN}",
    ]
    last_err = None
    for url in candidates:
        try:
            return http_get(url)
        except Exception as e:
            last_err = e
    raise last_err

def pick(obj, *keys):
    for k in keys:
        v = obj.get(k)
        if v not in (None, "", []):
            return v
    return None

def normalize_ticket(t):
    """Extrai campos usados nas tabelas de destino."""
    # responsável
    responsible_id = None
    responsible_name = None
    for k in ("owner", "responsible", "resolvedBy", "createdBy"):
        if isinstance(t.get(k), dict):
            responsible_id   = pick(t[k], "id", "personId", "agentId")
            responsible_name = pick(t[k], "businessName", "name", "fullName")
            if responsible_id or responsible_name:
                break

    origin = str(pick(t, "origin", "originId") or "")  # pode vir número
    status = pick(t, "status", "baseStatus")
    org_id = pick(t, "organizationId", "organization_id", "organizationIdOld")
    org_nm = pick(t, "organization", "organizationName", "organization_name")

    # ações (compactadas)
    actions = []
    for a in t.get("actions", []) or []:
        actions.append({
            "createdDate": a.get("createdDate") or a.get("createdAt"),
            "isPublic":    a.get("isPublic"),
            "type":        a.get("type"),
            "description": a.get("description") or "",
            "timeSpent":   a.get("timeSpent"),
            "createdBy":   (a.get("createdBy") or {}).get("id")
        })

    return {
        "ticket_id": int(t.get("id")),
        "responsible_id": responsible_id,
        "responsible_name": responsible_name,
        "origin": origin,
        "status": status,
        "organization_id": org_id,
        "organization_name": org_nm,
        "actions_json": json.dumps(actions, ensure_ascii=False)
    }

# ---------- DB work ----------

SQL_LATEST_RUN = """
    select max(id) from visualizacao_resolvidos.audit_recent_run
"""

SQL_MISSING_ROWS = """
    select table_name, ticket_id
      from visualizacao_resolvidos.audit_recent_missing
     where run_id = %s
"""

UPSERT_TICKETS_RESOLVIDOS = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id, responsible_id, responsible_name, origin, status, organization_id, organization_name)
values (%(ticket_id)s, %(responsible_id)s, %(responsible_name)s, %(origin)s, %(status)s, %(organization_id)s, %(organization_name)s)
on conflict (ticket_id) do update set
  responsible_id   = excluded.responsible_id,
  responsible_name = excluded.responsible_name,
  origin           = excluded.origin,
  status           = excluded.status,
  organization_id  = excluded.organization_id,
  organization_name= excluded.organization_name
"""

UPSERT_RESOLVIDOS_ACOES = """
insert into visualizacao_resolvidos.resolvidos_acoes
(ticket_id, acoes)
values (%(ticket_id)s, %(acoes_jsonb)s)
on conflict (ticket_id) do update set
  acoes = excluded.acoes
"""

UPSERT_DETAIL_CONTROL = """
insert into visualizacao_resolvidos.detail_control
(ticket_id, synced_at)
values (%s, now())
on conflict (ticket_id) do update set
  synced_at = excluded.synced_at
"""

def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        # 1) determine run
        if RUN_ID_OVERRIDE:
            run_id = int(RUN_ID_OVERRIDE)
        else:
            cur.execute(SQL_LATEST_RUN)
            run_id = cur.fetchone()[0]
        if not run_id:
            print("Nenhum run encontrado em audit_recent_run.")
            return
        print("Processando run_id:", run_id)

        # 2) rows missing
        cur.execute(SQL_MISSING_ROWS, (run_id,))
        rows = cur.fetchall()
        if not rows:
            print("Sem itens em audit_recent_missing para este run.")
            return

        need_tickets  = set()
        need_actions  = set()
        need_detail   = set()

        for table_name, tid in rows:
            if table_name == "tickets_resolvidos":
                need_tickets.add(tid)
            elif table_name == "resolvidos_acoes":
                need_actions.add(tid)
            elif table_name == "detail_control":
                need_detail.add(tid)

        all_ids = sorted(set().union(need_tickets, need_actions, need_detail))
        print(f"tickets_resolvidos pendentes: {len(need_tickets)}")
        print(f"resolvidos_acoes pendentes:   {len(need_actions)}")
        print(f"detail_control pendentes:     {len(need_detail)}")
        print(f"Total de tickets a consultar: {len(all_ids)}")

        # 3) baixa e normaliza (cache)
        norm_by_id = {}
        for i, tid in enumerate(all_ids, 1):
            try:
                t = get_ticket_full(tid)
                norm_by_id[tid] = normalize_ticket(t)
            except Exception as e:
                print(f"[WARN] ticket {tid}: {e}")
            time.sleep(THROTTLE)
            if i % 50 == 0:
                print(f"  ..{i}/{len(all_ids)} baixados")

        # 4) UPSERTS
        # tickets_resolvidos
        batch = []
        for tid in need_tickets:
            n = norm_by_id.get(tid)
            if not n: continue
            batch.append({
                "ticket_id": n["ticket_id"],
                "responsible_id": n["responsible_id"],
                "responsible_name": n["responsible_name"],
                "origin": n["origin"],
                "status": n["status"],
                "organization_id": n["organization_id"],
                "organization_name": n["organization_name"],
            })
        if batch:
            execute_batch(cur, UPSERT_TICKETS_RESOLVIDOS, batch, page_size=200)
            print(f"UPSERT tickets_resolvidos: {len(batch)}")

        # resolvidos_acoes
        batch = []
        for tid in need_actions:
            n = norm_by_id.get(tid)
            if not n: continue
            batch.append({
                "ticket_id": n["ticket_id"],
                "acoes_jsonb": psycopg2.extras.Json(json.loads(n["actions_json"]))
            })
        if batch:
            execute_batch(cur, UPSERT_RESOLVIDOS_ACOES, batch, page_size=200)
            print(f"UPSERT resolvidos_acoes: {len(batch)}")

        # detail_control
        if need_detail:
            execute_batch(cur, UPSERT_DETAIL_CONTROL, [(tid,) for tid in need_detail], page_size=500)
            print(f"UPSERT detail_control: {len(need_detail)}")

        print("Concluído com sucesso.")

if __name__ == "__main__":
    main()
