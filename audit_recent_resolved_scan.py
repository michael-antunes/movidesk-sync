# -*- coding: utf-8 -*-
import os, time, math, json, logging, datetime as dt
from urllib.parse import urlencode, quote
import requests, psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
WINDOW_HOURS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))  # 48 horas por padrão

def od_get(path: str, params: dict):
    params = {"token": TOKEN, **params}
    url = f"{BASE}/{path}?{urlencode(params, safe='(),$= ')}"
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    return r.json()

def page_iter(filter_expr: str, select_fields: str):
    # paginação OData do Movidesk com skip/take
    take = PAGE_SIZE
    skip = 0
    total = 0
    first = True
    while True:
        params = {
            "$filter": filter_expr,
            "$select": select_fields,
            "$expand": None,  # nada aqui no scan
            "$orderby": "id desc",
            "$skip": skip,
            "$take": take,
        }
        # removemos None
        params = {k: v for k, v in params.items() if v is not None}
        data = od_get("tickets", params)
        if first:
            total = len(data)  # não tem @count; usamos o próprio retorno
            first = False
        if not data:
            break
        yield data
        if len(data) < take:
            break
        skip += take
        time.sleep(THROTTLE)

def main():
    now = dt.datetime.now(dt.timezone.utc)
    win_end = now
    win_start = now - dt.timedelta(hours=WINDOW_HOURS)

    # tickets resolvidos OU fechados no intervalo
    # importante: aspas simples no OData. Nada de aspas duplas.
    f_start = win_start.isoformat()
    f_end   = win_end.isoformat()
    resolved = f"(resolvedIn ge {quote(f_start)} and resolvedIn le {quote(f_end)})"
    closed   = f"(closedIn ge {quote(f_start)} and closedIn le {quote(f_end)})"
    odata_filter = f"({resolved} or {closed})"
    select_fields = "id,baseStatus,resolvedIn,closedIn"

    logging.info("Janela: %s -> %s", f_start, f_end)
    total_api = 0
    ids = set()
    for page in page_iter(odata_filter, select_fields):
        for row in page:
            ids.add(int(row["id"]))
        total_api += len(page)

    logging.info("Total API (únicos): %d", len(ids))

    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        # inicia o run
        cur.execute("""
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at, window_from, window_to, total_local, notes)
            values (%s, %s, %s, 0, now(), %s, %s,
                    (select count(*) from visualizacao_resolvidos.tickets_resolvidos
                      where coalesce(last_resolved_at,last_closed_at) between %s and %s),
                    'scan resolved+closed')
            returning id
        """, (win_start, win_end, len(ids), win_start, win_end, win_start, win_end))
        run_id = cur.fetchone()[0]

        # compara com local e grava pendências por tabela
        missing_total = 0

        # 1) tickets_resolvidos
        cur.execute("""
            select ticket_id from visualizacao_resolvidos.tickets_resolvidos
            where coalesce(last_resolved_at,last_closed_at) between %s and %s
        """, (win_start, win_end))
        local_ids = {r[0] for r in cur.fetchall()}
        miss_tk = sorted(ids - local_ids)
        if miss_tk:
            execute_values(cur, """
                insert into visualizacao_resolvidos.audit_recent_missing (run_id, table_name, ticket_id, event_in, event_type)
                values %s
                on conflict do nothing
            """, [(run_id, "tickets_resolvidos", i, None, None) for i in miss_tk])
            missing_total += len(miss_tk)

        # 2) resolvidos_acoes (sempre que o ticket caiu na janela, esperamos ações)
        # checamos quais tickets já têm entrada em resolvidos_acoes
        cur.execute("""
            select ticket_id from visualizacao_resolvidos.resolvidos_acoes
            where ticket_id = any(%s)
        """, (list(ids),))
        ja_tem_acoes = {r[0] for r in cur.fetchall()}
        miss_acoes = sorted(set(ids) - ja_tem_acoes)
        if miss_acoes:
            execute_values(cur, """
                insert into visualizacao_resolvidos.audit_recent_missing (run_id, table_name, ticket_id, event_in, event_type)
                values %s
                on conflict do nothing
            """, [(run_id, "resolvidos_acoes", i, None, None) for i in miss_acoes])
            missing_total += len(miss_acoes)

        # fecha o run
        cur.execute("""
            update visualizacao_resolvidos.audit_recent_run
               set missing_total = %s
             where id = %s
        """, (missing_total, run_id))
        conn.commit()

        logging.info("Run %s: missing_total=%d", run_id, missing_total)

if __name__ == "__main__":
    main()
