import os, time, json, requests, psycopg2, psycopg2.extras
from datetime import datetime, timezone

API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

BATCH = int(os.getenv("ACTIONS_BATCH_SIZE","10"))
THROTTLE = float(os.getenv("ACTIONS_THROTTLE_SEC","0.7"))

UPSERT = """
insert into visualizacao_resolvidos.resolvidos_acoes
(ticket_id, acoes, updated_at)
values (%s, %s::jsonb, now())
on conflict (ticket_id) do update set
 acoes=excluded.acoes,
 updated_at=now()
"""

PICK = """
with tgt as (
  select tr.ticket_id
  from visualizacao_resolvidos.tickets_resolvidos tr
  left join visualizacao_resolvidos.resolvidos_acoes ra on ra.ticket_id = tr.ticket_id
  where ra.acoes is null or jsonb_typeof(ra.acoes) is null or (jsonb_typeof(ra.acoes)='array' and jsonb_array_length(ra.acoes)=0)
  order by coalesce(tr.last_update, tr.last_resolved_at, tr.last_closed_at, tr.last_cancelled_at) asc nulls last
  limit %s
)
select ticket_id from tgt
"""

def conn():
    return psycopg2.connect(NEON_DSN)

def req(url, params, retries=4):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.5*(i+1))
            continue
        r.raise_for_status()
    r.raise_for_status()

def compact_action(a):
    return {
        "id": a.get("id"),
        "isPublic": a.get("isPublic"),
        "description": a.get("description"),
        "createdDate": a.get("createdDate"),
        "origin": a.get("origin"),
        "attachments": a.get("attachments") or [],
        "timeAppointments": a.get("timeAppointments") or []
    }

def fetch_actions(ticket_id):
    url = "https://api.movidesk.com/public/v1/tickets"
    params = {
        "token": API_TOKEN,
        "$select": "id",
        "$expand": "actions($select=id,isPublic,description,createdDate,origin;$expand=attachments,timeAppointments)",
        "$filter": f"id eq {ticket_id}",
        "$top": 1
    }
    data = req(url, params) or []
    if not data:
        return []
    actions = data[0].get("actions") or []
    return [compact_action(a) for a in actions]

def cleanup_unique():
    sql = """
    do $$
    begin
      begin
        alter table visualizacao_resolvidos.resolvidos_acoes
        add constraint uq_resolvidos_acoes_ticket unique (ticket_id);
      exception when duplicate_table then
        null;
      end;
    end $$;
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql)

def heartbeat():
    sql = """
    insert into visualizacao_resolvidos.sync_control(name,last_update)
    values('default', now())
    on conflict (name) do update set last_update=now()
    """
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(sql)

def main():
    cleanup_unique()
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(PICK, (BATCH,))
            ids = [r[0] for r in cur.fetchall()]
    if not ids:
        heartbeat()
        return
    up_rows = []
    for tid in ids:
        try:
            acts = fetch_actions(tid)
        except Exception:
            time.sleep(THROTTLE)
            continue
        up_rows.append((tid, json.dumps(acts)))
        time.sleep(THROTTLE)
    if up_rows:
        with conn() as c:
            with c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, up_rows, page_size=50)
    heartbeat()

if __name__ == "__main__":
    main()
