# -*- coding: utf-8 -*-
import os
import time
import logging
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"

TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]

THROTTLE     = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
PAGE_SIZE    = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))
WINDOW_HOURS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))

def od_get(path: str, params: dict):
    """Chamada OData usando params= (requests monta a querystring)."""
    url = f"{BASE}/{path}"
    r = requests.get(url, params=params, timeout=60)
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

def iso_odata_z(d: dt.datetime) -> str:
    """DateTimeOffset em UTC sem aspas, no formato aceito pelo OData: YYYY-MM-DDTHH:MM:SSZ."""
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    d = d.astimezone(dt.timezone.utc).replace(microsecond=0)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    now = dt.datetime.now(dt.timezone.utc)
    win_end = now
    win_start = now - dt.timedelta(hours=WINDOW_HOURS)

    f_start = iso_odata_z(win_start)
    f_end   = iso_odata_z(win_end)

    # IMPORTANTE: SEM aspas – DateTimeOffset literal
    resolved = f"(resolvedIn ge {f_start} and resolvedIn le {f_end})"
    closed   = f"(closedIn  ge {f_start} and closedIn  le {f_end})"
    odata_filter = f"({resolved} or {closed})"
    select_fields = "id,baseStatus,resolvedIn,closedIn"

    logging.info("Janela: %s -> %s", f_start, f_end)

    # 1) IDs únicos no período
    ids = set()
    for page in page_iter(odata_filter, select_fields):
        for row in page:
            try:
                ids.add(int(row["id"]))
            except Exception:
                pass

    logging.info("Total API (únicos): %d", len(ids))

    # 2) Abre RUN e registra pendências
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at, window_from, window_to,
               total_local, notes)
            VALUES (%s, %s, %s, 0, now(), %s, %s,
                    (
                        SELECT count(*)
                        FROM visualizacao_resolvidos.tickets_resolvidos_detail d
                        WHERE COALESCE(
                                NULLIF(d.raw ->> 'resolvedIn', '')::timestamptz,
                                NULLIF(d.raw ->> 'closedIn',   '')::timestamptz
                              ) BETWEEN %s AND %s
                    ),
                    'scan resolved+closed')
            RETURNING id
        """, (win_start, win_end, len(ids), win_start, win_end, win_start, win_end))
        run_id = cur.fetchone()[0]

        missing_total = 0

        # pendências em tickets_resolvidos_detail dentro da janela
        cur.execute("""
            SELECT d.ticket_id
              FROM visualizacao_resolvidos.tickets_resolvidos_detail d
             WHERE COALESCE(
                     NULLIF(d.raw ->> 'resolvedIn', '')::timestamptz,
                     NULLIF(d.raw ->> 'closedIn',   '')::timestamptz
                   ) BETWEEN %s AND %s
        """, (win_start, win_end))
        local_ids = {r[0] for r in cur.fetchall()}
        miss_tk = sorted(ids - local_ids)
        if miss_tk:
            execute_values(cur, """
                INSERT INTO visualizacao_resolvidos.audit_recent_missing
                    (run_id, table_name, ticket_id)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, [(run_id, "tickets_resolvidos_detail", i) for i in miss_tk])
            missing_total += len(miss_tk)

        # pendências em resolvidos_acoes para os IDs encontrados
        if ids:
            cur.execute("""
                SELECT ticket_id
                  FROM visualizacao_resolvidos.resolvidos_acoes
                 WHERE ticket_id = ANY(%s)
            """, (list(ids),))
            ja_tem = {r[0] for r in cur.fetchall()}
            miss_acoes = sorted(ids - ja_tem)
            if miss_acoes:
                execute_values(cur, """
                    INSERT INTO visualizacao_resolvidos.audit_recent_missing
                        (run_id, table_name, ticket_id)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, [(run_id, "resolvidos_acoes", i) for i in miss_acoes])
            missing_total += len(miss_acoes)

        # fecha RUN
        cur.execute("""
            UPDATE visualizacao_resolvidos.audit_recent_run
               SET missing_total = %s
             WHERE id = %s
        """, (missing_total, run_id))
        conn.commit()

        logging.info("Run %s finalizado. missing_total=%d", run_id, missing_total)

if __name__ == "__main__":
    main()
