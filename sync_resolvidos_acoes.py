import os
import time
import json
import requests
import psycopg2

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

CHAT_ORIGINS = {"5", "6", "23", "25", "26", "27"}


def _req(url, params, timeout=90):
    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:800])
            except Exception:
                pass
            r.raise_for_status()
        if not r.text:
            return []
        return r.json()


def select_ticket_ids_to_update(conn, limit_):
    sql = """
        select tr.ticket_id
        from visualizacao_resolvidos.tickets_resolvidos tr
        join visualizacao_resolvidos.resolvidos_acoes ra
          on ra.ticket_id = tr.ticket_id
        where tr.origin in ('5','6','23','25','26','27')
        order by tr.ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_ticket_actions_with_html(ticket_id):
    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        "$select": "id,actions",
        "$expand": "actions($select=id,origin,type,status,createdDate,createdBy,description,htmlDescription,attachments)",
    }
    url = f"{API_BASE}/tickets"
    data = _req(url, params) or []
    if isinstance(data, list):
        ticket = data[0] if data else {}
    else:
        ticket = data if isinstance(data, dict) else {}
    actions = ticket.get("actions") or []
    return actions


def update_resolvidos_acoes(conn, ticket_id, actions):
    payload = json.dumps(actions, ensure_ascii=False)
    sql = """
        update visualizacao_resolvidos.resolvidos_acoes
           set acoes = %s
         where ticket_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (payload, ticket_id))


def sync_resolvidos_acoes(conn, limit_):
    ticket_ids = select_ticket_ids_to_update(conn, limit_)
    if not ticket_ids:
        print("RESOLVIDOS_ACOES: nenhum ticket para atualizar")
        return 0
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "5.0"))
    updated = 0
    for idx, ticket_id in enumerate(ticket_ids, start=1):
        try:
            actions = fetch_ticket_actions_with_html(ticket_id)
            update_resolvidos_acoes(conn, ticket_id, actions)
            updated += 1
            print(
                f"RESOLVIDOS_ACOES: ticket {ticket_id} atualizado com {len(actions)} ações"
            )
        except Exception as exc:
            print(f"RESOLVIDOS_ACOES ERRO ticket {ticket_id}: {exc}")
        if idx < len(ticket_ids):
            time.sleep(throttle)
    return updated


def main():
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não definido")
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não definido")

    limit_ids = int(os.getenv("MOVIDESK_ACOES_LIMIT", "100"))

    conn = psycopg2.connect(NEON_DSN)
    conn.autocommit = True
    try:
        n = sync_resolvidos_acoes(conn, limit_ids)
        print(f"RESOLVIDOS_ACOES: {n} tickets atualizados")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
