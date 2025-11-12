#!/usr/bin/env python3
import os, time, json, sys, requests, psycopg2, psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN", "")
NEON_DSN  = os.getenv("NEON_DSN", "")
THROTTLE  = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
TOP       = int(os.getenv("MOVIDESK_TOP", "10"))
TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "30"))

def req(url, params=None):
    if params is None:
        params = {}
    params = {k: v for k, v in params.items() if v is not None}
    for attempt in range(4):
        r = requests.get(url, params=params, timeout=TIMEOUT)
        if r.status_code >= 500:
            time.sleep(0.5 * (attempt + 1))
            continue
        r.raise_for_status()
        time.sleep(THROTTLE)
        return r.json()
    r.raise_for_status()

def conn():
    return psycopg2.connect(NEON_DSN)

def cleanup_dupes(c):
    sql = """
    with ranked as (
      select ctid,
             row_number() over(
               partition by tr.ticket_id
               order by greatest(
                        coalesce(tr.last_update,        '-infinity'::timestamptz),
                        coalesce(tr.last_resolved_at,   '-infinity'::timestamptz),
                        coalesce(tr.last_closed_at,     '-infinity'::timestamptz),
                        coalesce(tr.last_cancelled_at,  '-infinity'::timestamptz)
                      ) desc,
                      tr.ticket_id desc
             ) rn
      from visualizacao_resolvidos.tickets_resolvidos tr
    )
    delete from visualizacao_resolvidos.tickets_resolvidos t
    using ranked r
    where t.ctid = r.ctid and r.rn > 1;
    """
    with c.cursor() as cur:
        cur.execute(sql)
    c.commit()

def next_ticket_ids(c, limit=10):
    sql = """
    with latest as (
      select *
      from (
        select tr.*,
               row_number() over(
                 partition by tr.ticket_id
                 order by greatest(
                          coalesce(tr.last_update,        '-infinity'::timestamptz),
                          coalesce(tr.last_resolved_at,   '-infinity'::timestamptz),
                          coalesce(tr.last_closed_at,     '-infinity'::timestamptz),
                          coalesce(tr.last_cancelled_at,  '-infinity'::timestamptz)
                        ) desc,
                        tr.ticket_id desc
               ) rn
        from visualizacao_resolvidos.tickets_resolvidos tr
      ) x
      where x.rn = 1
    )
    select l.ticket_id
    from latest l
    left join visualizacao_resolvidos.resolvidos_acoes ra
           on ra.ticket_id = l.ticket_id
    where ra.ticket_id is null
       or coalesce(jsonb_array_length(ra.acoes), 0) = 0
    order by greatest(
             coalesce(l.last_update,        '-infinity'::timestamptz),
             coalesce(l.last_resolved_at,   '-infinity'::timestamptz),
             coalesce(l.last_closed_at,     '-infinity'::timestamptz),
             coalesce(l.last_cancelled_at,  '-infinity'::timestamptz)
           ) desc,
           l.ticket_id desc
    limit %s;
    """
    with c.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]

def upsert_acoes(c, rows):
    if not rows:
        return
    sql = """
    insert into visualizacao_resolvidos.resolvidos_acoes (ticket_id, acoes)
    values (%s, %s)
    on conflict (ticket_id) do update
      set acoes = excluded.acoes;
    """
    with c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    c.commit()

def heartbeat(c):
    sql = """
      insert into visualizacao_resolvidos.sync_control(name,last_update)
      values ('default', now())
      on conflict (name) do update set last_update = excluded.last_update
    """
    with c.cursor() as cur:
        cur.execute(sql)
    c.commit()

def fetch_actions_for_ticket(ticket_id: int):
    expand = "actions($expand=attachments,timeAppointments;$select=id,isPublic,description,createdDate,origin,attachments,timeAppointments)"
    data = req(f"{API_BASE}/tickets/{ticket_id}",
               {"token": API_TOKEN, "$select": "actions", "$expand": expand}) or {}
    actions = data.get("actions") or []
    norm = []
    for a in actions:
        norm.append({
            "id": a.get("id"),
            "isPublic": a.get("isPublic"),
            "description": a.get("description"),
            "htmlDescription": a.get("htmlDescription"),
            "createdDate": a.get("createdDate"),
            "origin": a.get("origin"),
            "attachments": [
                {"id": att.get("id"), "fileName": att.get("fileName")}
                for att in (a.get("attachments") or [])
            ],
            "timeAppointments": [
                {"id": t.get("id"), "workingTime": t.get("workingTime")}
                for t in (a.get("timeAppointments") or [])
            ]
        })
    if any(x.get("htmlDescription") is None for x in norm):
        try:
            html_payload = None
            try:
                html_payload = req(f"{API_BASE}/tickets/htmldescription/{ticket_id}",
                                   {"token": API_TOKEN})
            except requests.HTTPError:
                html_payload = req(f"{API_BASE}/tickets/htmldescription",
                                   {"token": API_TOKEN, "ticketId": ticket_id})
            if isinstance(html_payload, dict) and "actions" in html_payload:
                html_actions = {a.get("id"): a.get("htmlDescription") for a in html_payload["actions"]}
                for item in norm:
                    if item.get("htmlDescription") is None and item.get("id") in html_actions:
                        item["htmlDescription"] = html_actions[item["id"]]
        except Exception:
            pass
    return norm

def main():
    if not API_TOKEN or not NEON_DSN:
        print("ERRO_ENV", file=sys.stderr)
        sys.exit(1)
    c = conn()
    try:
        cleanup_dupes(c)
        ids = next_ticket_ids(c, TOP)
        if not ids:
            heartbeat(c)
            print("OK 0")
            return
        rows = []
        for tid in ids:
            try:
                acoes = fetch_actions_for_ticket(tid)
            except requests.HTTPError as e:
                acoes = []
            rows.append((tid, json.dumps(acoes, ensure_ascii=False)))
        upsert_acoes(c, rows)
        heartbeat(c)
        print(f"OK {len(rows)} {ids}")
    finally:
        c.close()

if __name__ == "__main__":
    main()
