import os
import time
import json
import requests
import psycopg2
import psycopg2.extras
import datetime

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

CHAT_ORIGINS = {5, 6, 23, 25, 26, 27}


def to_iso_z(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iint(x):
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _req(url, params, max_retries=3):
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    last_exc = None
    for _ in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=90)
            if r.status_code == 200:
                return r.json()
            last_exc = RuntimeError(f"HTTP {r.status_code}: {r.text}")
        except Exception as exc:
            last_exc = exc
        time.sleep(throttle)
    if last_exc:
        raise last_exc
    return None


def get_since_from_db(conn, days_back, overlap_minutes):
    with conn.cursor() as cur:
        cur.execute(
            "select max(changed_date) from visualizacao_resolucao.resolucao_por_status"
        )
        row = cur.fetchone()
    now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    if row and row[0]:
        since = row[0]
        if since.tzinfo is None:
            since = since.replace(tzinfo=datetime.timezone.utc)
        since = since - datetime.timedelta(minutes=overlap_minutes)
    else:
        since = now_utc - datetime.timedelta(days=days_back)
    return since


def fetch_status_history(since_iso):
    url = f"{API_BASE}/tickets/statusHistory"
    limit = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
    starting_after = None
    items = []
    while True:
        params = {
            "token": API_TOKEN,
            "changedDateGreaterThan": since_iso,
            "limit": limit,
        }
        if starting_after:
            params["startingAfter"] = starting_after
        page = _req(url, params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(float(os.getenv("MOVIDESK_THROTTLE", "0.2")))
    return items


def map_row(r):
    return {
        "id": r.get("id"),
        "ticket_id": r.get("ticketId"),
        "agent_name": r.get("agentName"),
        "changed_date": r.get("changedDate"),
        "status": r.get("status"),
        "permanency_time_fulltime_seconds": iint(
            r.get("permanencyTimeFulltimeSeconds")
        ),
    }


UPSERT_SQL = """
insert into visualizacao_resolucao.resolucao_por_status
(id,ticket_id,agent_name,changed_date,status,permanency_time_fulltime_seconds)
values (%(id)s,%(ticket_id)s,%(agent_name)s,%(changed_date)s,%(status)s,%(permanency_time_fulltime_seconds)s)
on conflict (id) do update set
  ticket_id = excluded.ticket_id,
  agent_name = excluded.agent_name,
  changed_date = excluded.changed_date,
  status = excluded.status,
  permanency_time_fulltime_seconds = excluded.permanency_time_fulltime_seconds
"""


def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=1000)
    return len(rows)


def select_ticket_ids_chat_to_update(conn, limit_):
    sql = """
        select ra.ticket_id
        from visualizacao_resolvidos.resolvidos_acoes ra
        join visualizacao_resolvidos.tickets_resolvidos tr
          on tr.ticket_id = ra.ticket_id
        where tr.origin in (23, 25, 26, 27)
          and jsonb_typeof(ra.acoes) = 'array'
          and (
              (ra.acoes->0->>'description') is null
              or ra.acoes->0->>'description' = ''
          )
        order by ra.ticket_id
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_ticket_chat_actions(ticket_id):
    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        "$select": "id,actions",
        "$expand": "actions($select=id,origin,type,status,createdDate,createdBy,description,attachments)",
    }
    url = f"{API_BASE}/tickets"
    r = requests.get(url, params=params, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(
            f"Movidesk API error for ticket {ticket_id}: {r.status_code} {r.text}"
        )
    data = r.json()
    if isinstance(data, list):
        ticket = data[0] if data else {}
    else:
        ticket = data
    actions = ticket.get("actions") or []
    result = []
    for a in actions:
        if a.get("origin") in CHAT_ORIGINS:
            result.append(a)
    return result


def update_resolvidos_acoes_chat(conn, ticket_id, actions):
    payload = json.dumps(actions, ensure_ascii=False)
    sql = """
        update visualizacao_resolvidos.resolvidos_acoes
           set acoes = %s
         where ticket_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (payload, ticket_id))


def sync_chat_resolvidos_acoes(conn, limit_):
    ticket_ids = select_ticket_ids_chat_to_update(conn, limit_)
    if not ticket_ids:
        print("SYNC CHAT: nenhum ticket para atualizar")
        return 0
    updated = 0
    for idx, ticket_id in enumerate(ticket_ids, start=1):
        try:
            actions = fetch_ticket_chat_actions(ticket_id)
            update_resolvidos_acoes_chat(conn, ticket_id, actions)
            updated += 1
            print(
                f"SYNC CHAT: ticket {ticket_id} atualizado com {len(actions)} ações de chat"
            )
        except Exception as exc:
            print(f"SYNC CHAT ERRO ticket {ticket_id}: {exc}")
        if idx < len(ticket_ids):
            time.sleep(6)
    return updated


def main():
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não definido")
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não definido")

    days_back = int(os.getenv("MOVIDESK_RPS_DAYS", "120"))
    overlap_minutes = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))
    chat_limit = int(os.getenv("MOVIDESK_CHAT_LIMIT", "200"))

    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    try:
        since_dt = get_since_from_db(conn, days_back, overlap_minutes)
        since_iso = to_iso_z(since_dt)

        resp = fetch_status_history(since_iso)
        rows = [map_row(x) for x in resp if isinstance(x, dict)]
        n = upsert_rows(conn, rows)

        print(f"DESDE {since_iso} | UPSERT RPS: {n}")

        n_chat = sync_chat_resolvidos_acoes(conn, chat_limit)
        print(f"SYNC CHAT RESOLVIDOS_ACOES: {n_chat}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
