import os
import time
import json
import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")
BATCH = int(os.getenv("DETAIL_BATCH", "200"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/detail"})


def md_get(path_or_full, params=None, ok_404=False):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN
    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or {}
    if ok_404 and r.status_code == 404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or {}
    r.raise_for_status()


SQL_GET_PENDING = """
select ticket_id
  from visualizacao_resolvidos.audit_recent_missing
 where table_name = 'tickets_resolvidos'
 group by ticket_id
 order by max(run_id) desc, ticket_id desc
 limit %s
"""

SQL_DELETE_MISSING = """
delete from visualizacao_resolvidos.audit_recent_missing
 where table_name = 'tickets_resolvidos'
   and ticket_id = any(%s)
"""


def register_ticket_failure(conn, ticket_id, reason):
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from visualizacao_resolvidos.audit_ticket_watch
             where table_name = 'tickets_resolvidos'
               and ticket_id = %s
            """,
            (ticket_id,),
        )
        cur.execute(
            """
            insert into visualizacao_resolvidos.audit_ticket_watch(table_name, ticket_id, last_seen_at)
            values ('tickets_resolvidos', %s, now())
            """,
            (ticket_id,),
        )
    conn.commit()


def get_pending_ids(conn, limit):
    with conn.cursor() as cur:
        cur.execute(SQL_GET_PENDING, (limit,))
        return [r[0] for r in cur.fetchall()]


def build_detail_row(ticket):
    owner = ticket.get("owner") or {}
    org = ticket.get("organization") or {}
    clients = ticket.get("clients") or []
    client = clients[0] if clients else {}
    return (
        int(ticket["id"]),
        ticket.get("status"),
        ticket.get("lastResolvedDate"),
        ticket.get("lastClosedDate"),
        ticket.get("lastCancelledDate"),
        ticket.get("lastUpdate"),
        ticket.get("origin"),
        ticket.get("category"),
        ticket.get("urgency"),
        ticket.get("serviceFirstLevel"),
        ticket.get("serviceSecondLevel"),
        ticket.get("serviceThirdLevel"),
        owner.get("id"),
        owner.get("businessName"),
        owner.get("team"),
        org.get("id"),
        org.get("businessName"),
        ticket.get("subject"),
        client.get("businessName"),
    )


def upsert_details(conn, rows):
    if not rows:
        return
    sql = """
    insert into visualizacao_resolvidos.tickets_resolvidos (
      ticket_id,
      status,
      last_resolved_at,
      last_closed_at,
      last_cancelled_at,
      last_update,
      origin,
      category,
      urgency,
      service_first_level,
      service_second_level,
      service_third_level,
      owner_id,
      owner_name,
      owner_team_name,
      organization_id,
      organization_name,
      subject,
      adicional_nome
    )
    values %s
    on conflict (ticket_id) do update set
      status               = excluded.status,
      last_resolved_at     = excluded.last_resolved_at,
      last_closed_at       = excluded.last_closed_at,
      last_cancelled_at    = excluded.last_cancelled_at,
      last_update          = excluded.last_update,
      origin               = excluded.origin,
      category             = excluded.category,
      urgency              = excluded.urgency,
      service_first_level  = excluded.service_first_level,
      service_second_level = excluded.service_second_level,
      service_third_level  = excluded.service_third_level,
      owner_id             = excluded.owner_id,
      owner_name           = excluded.owner_name,
      owner_team_name      = excluded.owner_team_name,
      organization_id      = excluded.organization_id,
      organization_name    = excluded.organization_name,
      subject              = excluded.subject,
      adicional_nome       = excluded.adicional_nome
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()


def delete_processed_from_missing(conn, ids):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_MISSING, (ids,))
    conn.commit()


def main():
    with psycopg2.connect(DSN) as conn:
        pending = get_pending_ids(conn, BATCH)
        total_pendentes = len(pending)
        if not pending:
            print("detail: nenhum ticket pendente em audit_recent_missing.")
            return

        print(f"detail: {total_pendentes} tickets pendentes em audit_recent_missing (limite={BATCH}).")

        detalhes = []
        ok_ids = []
        fail_reasons = {}
        fail_samples = {}

        for tid in pending:
            reason = None

            try:
                data = md_get(
                    f"tickets/{tid}",
                    params={"$expand": "clients,createdBy,owner,actions,customFields"},
                    ok_404=True,
                )
            except requests.HTTPError as e:
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                reason = f"http_error_{status or 'unknown'}"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue
            except Exception:
                reason = "exception_api"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if isinstance(data, list):
                if not data:
                    reason = "empty_list"
                    register_ticket_failure(conn, tid, reason)
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    if reason not in fail_samples:
                        fail_samples[reason] = tid
                    continue
                ticket = data[0]
            else:
                ticket = data

            if not ticket.get("id"):
                reason = "missing_id"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            detalhes.append(row)
            ok_ids.append(tid)
            time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = sum(fail_reasons.values())

        print(f"detail: processados neste ciclo: ok={total_ok}, falhas={total_fail}.")

        if fail_reasons:
            print("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                print(f"  - {r}: {c} tickets (exemplo ticket_id={sample})")

        if not detalhes:
            print("detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch.")
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        print(f"detail: {total_ok} tickets upsertados em tickets_resolvidos e removidos do missing.")


if __name__ == "__main__":
    main()
