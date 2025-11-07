# -*- coding: utf-8 -*-
import os, time, logging, datetime as dt
from urllib.parse import urlencode
import requests, psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
WINDOW_HOURS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))

def od_get(path: str, params: dict):
    # permite caracteres do OData no querystring
    url = f"{BASE}/{path}?{urlencode(params, safe=\"(),$=':+TZ- \")}"
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    return r.json()

def page_iter(filter_expr: str, select_fields: str):
    """Paginação com $top/$skip (Movidesk não suporta $take)."""
    top = PAGE_SIZE
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "$filter": filter_expr,
            "$select": select_fields,
            "$orderby": "id desc",
            "$skip": skip,
            "$top": top,
        }
        data = od_get("tickets", params)
        if not data:
            break
        yield data
        if len(data) < top:
            break
        skip += top
        time.sleep(THROTTLE)

def main():
    now = dt.datetime.now(dt.timezone.utc)
    win_end = now
    win_start = now - dt.timedelta(hours=WINDOW_HOURS)

    def iso(z):  # sem micros, com timezone
        return z.replace(microsecond=0).isoformat()

    f_start = iso(win_start)
    f_end   = iso(win_end)

    # datas entre ASPAS SIMPLES
    resolved = f"(resolvedIn ge '{f_start}' and resolvedIn le '{f_end}')"
    closed   = f"(closedIn  ge '{f_start}' and closedIn  le '{f_end}')"
    odata_filter = f"({resolved} or {closed})"
    select_fields = "id,baseStatus,resolvedIn,closedIn"

    logging.info("Janela: %s -> %s", f_start, f_end)

    ids = set()
    for page in page_iter(odata_filter, select_fields):
        for row in page:
            try:
                ids.add(int(row["id"]))
            except Exception:
                pass

    logging.info("Total API (únicos): %d", len(ids))

    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        # abre RUN
        cur.execute("""
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at, window_from, window_to,
               total_local, notes)
            values (%s, %s, %s, 0, now(), %s, %s,
                    (select count(*) from visualizacao_resolvidos.tickets_resolvidos
                      where coalesce(last_resolved_at,last_closed_at) between %s and %s),
                    'scan resolved+closed')
            returning id
        """, (win_start, win_end, len(ids), win_start, win_end, win_start, win_end))
        run_id = cur.fetchone()[0]

        missing_total = 0

        # pendência em tickets_resolvidos
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.tickets_resolvidos
             where coalesce(last_resolved_at,last_closed_at) between %s and %s
        """, (win_start, win_end))
        local_ids = {r[0] for r in cur.fetchall()}
        miss_tk = sorted(ids - local_ids)
        if miss_tk:
            execute_values(cur, """
                insert into visualizacao_resolvidos.audit_recent_missing
                    (run_id, table_name, ticket_id)
                values %s
                on conflict do nothing
            """, [(run_id, "tickets_resolvidos", i) for i in miss_tk])
            missing_total += len(miss_tk)

        # pendência em resolvidos_acoes
        cur.execute("""
            select ticket_id
              from visualizacao_resolvidos.resolvidos_acoes
             where ticket_id = any(%s)
        """, (list(ids),))
        ja_tem = {r[0] for r in cur.fetchall()}
        miss_acoes = sorted(ids - ja_tem)
        if miss_acoes:
            execute_values(cur, """
                insert into visualizacao_resolvidos.audit_recent_missing
                    (run_id, table_name, ticket_id)
                values %s
                on conflict do nothing
            """, [(run_id, "resolvidos_acoes", i) for i in miss_acoes])
            missing_total += len(miss_acoes)

        # fecha RUN
        cur.execute("""
            update visualizacao_resolvidos.audit_recent_run
               set missing_total = %s
             where id = %s
        """, (missing_total, run_id))
        conn.commit()

        logging.info("Run %s: missing_total=%d", run_id, missing_total)

if __name__ == "__main__":
    main()
