import os, time, json, requests, psycopg2, psycopg2.extras

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
with uniq as (
  select tr.*,
         row_number() over (
           partition by tr.ticket_id
           order by coalesce(tr.last_update, tr.last_resolved_at, tr.last_closed_at, tr.last_cancelled_at) desc,
                    tr.ticket_id desc
         ) rn
  from visualizacao_resolvidos.tickets_resolvidos tr
),
latest as (
  select * from uniq where rn = 1
)
select l.ticket_id
from latest l
left join visualizacao_resolvidos.resolvidos_acoes ra on ra.ticket_id = l.ticket_id
where ra.ticket_id is null
   or coalesce(jsonb_array_length(ra.acoes),0)=0
order by coalesce(l.last_update, l.last_resolved_at, l.last_closed_at, l.last_cancelled_at) desc nulls last,
         l.ticket_id desc
limit %s
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

def fetch_actions_json(ticket_id):
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
    return actions

def fetch_html_for_action(ticket_id, action_id):
    try:
        o = req("https://api.movidesk.com/public/v1/tickets/htmldescription",
                {"token": API_TOKEN, "id": str(ticket_id), "actionId": str(action_id)})
        if isinstance(o, dict):
            return o.get("description")
    except:
        return None
    return None

def enrich_with_html(ticket_id, actions):
    out = []
    for a in actions:
        obj = dict(a)
        if obj.get("description"):
            try:
                time.sleep(THROTTLE)
                html = fetch_html_for_action(ticket_id, obj.get("id"))
                if html:
                    obj["html"] = html
            except:
                pass
        out.append(obj)
    return out

def main():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(PICK, (BATCH,))
            ids = [r[0] for r in cur.fetchall()]
    if not ids:
        return
    rows = []
    for tid in ids:
        try:
            acts = fetch_actions_json(tid)
            acts = enrich_with_html(tid, acts)
            rows.append((tid, json.dumps(acts, ensure_ascii=False)))
        except Exception:
            time.sleep(THROTTLE)
            continue
        time.sleep(THROTTLE)
    if rows:
        with conn() as c:
            with c.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT, rows, page_size=50)

if __name__ == "__main__":
    main()
