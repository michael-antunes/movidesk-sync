import os
import requests
import psycopg2
import psycopg2.extras

NEON_DSN = os.environ["NEON_DATABASE_URL"]
MOVIDESK_BASE_URL = "https://api.movidesk.com/public/v1/tickets"
MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]


def get_conn():
    return psycopg2.connect(NEON_DSN, sslmode="require")


def select_missing_ticket_ids(conn, limit):
    sql = """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = 'tickets_resolvidos'
        order by ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_ticket_detail(ticket_id):
    params = {
        "token": MOVIDESK_TOKEN,
        "id": ticket_id
    }
    resp = requests.get(f"{MOVIDESK_BASE_URL}/{ticket_id}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_row(ticket_json):
    ticket_id = ticket_json["id"]
    owner_name = ticket_json.get("owner", {}).get("businessName")
    owner_team_name = ticket_json.get("ownerTeam", {}).get("name")
    origin = ticket_json.get("origin")
    importance = ticket_json.get("importance")
    urgency = ticket_json.get("urgency")
    status = ticket_json.get("status")
    subject = ticket_json.get("subject")
    created_date = ticket_json.get("createdDate")
    resolved_in = ticket_json.get("resolvedIn")
    return (
        ticket_id,
        owner_name,
        owner_team_name,
        origin,
        importance,
        urgency,
        status,
        subject,
        created_date,
        resolved_in,
    )


UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos_detail (
    ticket_id,
    owner_name,
    owner_team_name,
    origin,
    importance,
    urgency,
    status,
    subject,
    created_date,
    resolved_in
) values (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
on conflict (ticket_id) do update set
    owner_name = excluded.owner_name,
    owner_team_name = excluded.owner_team_name,
    origin = excluded.origin,
    importance = excluded.importance,
    urgency = excluded.urgency,
    status = excluded.status,
    subject = excluded.subject,
    created_date = excluded.created_date,
    resolved_in = excluded.resolved_in
"""


def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def delete_from_audit(conn, ticket_ids):
    if not ticket_ids:
        return
    sql = """
        delete from visualizacao_resolvidos.audit_recent_missing
        where table_name = 'tickets_resolvidos'
          and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_ids,))
    conn.commit()


def process_batch(conn, ticket_ids):
    rows = []
    for tid in ticket_ids:
        data = fetch_ticket_detail(tid)
        rows.append(normalize_row(data))
    upsert_rows(conn, rows)
    delete_from_audit(conn, ticket_ids)


def process_with_skip(conn, all_ticket_ids, batch_size=100):
    skipped = []

    def safe_process(batch):
        nonlocal skipped
        if not batch:
            return
        try:
            process_batch(conn, batch)
        except requests.HTTPError as e:
            status = e.response.status_code
            if status == 400 and len(batch) > 1:
                mid = len(batch) // 2
                safe_process(batch[:mid])
                safe_process(batch[mid:])
            elif status == 400 and len(batch) == 1:
                skipped.append(batch[0])
            else:
                raise

    for i in range(0, len(all_ticket_ids), batch_size):
        safe_process(all_ticket_ids[i:i + batch_size])

    return skipped


def main():
    conn = get_conn()
    try:
        missing_ids = select_missing_ticket_ids(conn, 5000)
        print(f"Reprocessando {len(missing_ids)} tickets da audit")
        skipped = process_with_skip(conn, missing_ids, batch_size=100)
        if skipped:
            print(f"Tickets ignorados por erro 400: {skipped}")
        else:
            print("Todos os tickets do lote foram processados")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
