# audit_recent_resolved_scan.py
import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN  = os.getenv("NEON_DSN")

PAGE_TOP   = int(os.getenv("MOVIDESK_PAGE_SIZE", "1000"))   # pedido: 1000
THROTTLE   = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
WINDOW_HRS = int(os.getenv("AUDIT_WINDOW_HOURS", "48"))     # 48h por padrão

SCHEMA = "visualizacao_resolvidos"
TABLE_RUN     = f"{SCHEMA}.audit_recent_run"
TABLE_MISSING = f"{SCHEMA}.audit_recent_missing"

TABLES_TO_CHECK = [
    f"{SCHEMA}.tickets_resolvidos",
    f"{SCHEMA}.resolvidos_acoes",
    f"{SCHEMA}.detail_control",
]

http = requests.Session()
http.headers.update({"Accept": "application/json"})

def iso_z(dt):
    # Sempre sem aspas e com 'Z' no fim
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def now_utc():
    import datetime as _dt
    return _dt.datetime.utcnow().replace(tzinfo=_dt.timezone.utc)

def movidesk_get_tickets(token, dt_from, dt_to):
    """Retorna lista de IDs de tickets com resolvedIn OU closedIn dentro do intervalo."""
    url = f"{API_BASE}/tickets"
    ids = []
    skip = 0

    ts_from = iso_z(dt_from)
    ts_to   = iso_z(dt_to)

    # Filtro alvo (sem aspas) – o que a API aceita
    filter_noquotes = (
        f"((resolvedIn ge {ts_from} and resolvedIn le {ts_to}) "
        f"or (closedIn ge {ts_from} and closedIn le {ts_to}))"
    )
    # fallback com aspas simples (caso alguma instância aceite esse formato)
    filter_singlequotes = (
        f"((resolvedIn ge '{ts_from}' and resolvedIn le '{ts_to}') "
        f"or (closedIn ge '{ts_from}' and closedIn le '{ts_to}'))"
    )

    def fetch_page(the_filter, _skip):
        params = {
            "token": token,
            "$select": "id,resolvedIn,closedIn,lastUpdate",
            "$filter": the_filter,
            "$top": PAGE_TOP,
            "$skip": _skip,
        }
        r = http.get(url, params=params, timeout=120)
        return r

    # Primeira tentativa: sem aspas
    filter_in_use = filter_noquotes
    tried_fallback = False

    while True:
        r = fetch_page(filter_in_use, skip)

        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue

        if r.status_code == 400 and not tried_fallback:
            # Tenta com aspas simples se vier erro de sintaxe
            tried_fallback = True
            filter_in_use = filter_singlequotes
            continue

        r.raise_for_status()
        page = r.json() if r.text else []
        if not isinstance(page, list) or not page:
            break

        for t in page:
            tid = t.get("id")
            if isinstance(tid, str) and tid.isdigit():
                tid = int(tid)
            ids.append(tid)

        if len(page) < PAGE_TOP:
            break

        skip += len(page)
        time.sleep(THROTTLE)

    # Dedup
    ids = sorted({i for i in ids if isinstance(i, int)})
    return ids

def get_ticket_id_column(cur, full_table_name):
    """Descobre se a tabela usa 'ticket_id' ou 'id'."""
    schema, table = full_table_name.split(".", 1)
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
          and column_name in ('ticket_id','id')
        order by case column_name when 'ticket_id' then 0 else 1 end
        limit 1
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Tabela {full_table_name} não tem coluna 'ticket_id' ou 'id'.")
    return row[0]

def compute_missing_for_table(conn, full_table_name, ids):
    if not ids:
        return []
    with conn.cursor() as cur:
        col = get_ticket_id_column(cur, full_table_name)
        # Usa UNNEST para comparar com o conjunto de IDs da API
        sql = f"""
            with api_ids as (
                select unnest(%s::int[]) as tid
            )
            select a.tid
            from api_ids a
            left join {full_table_name} t
              on t.{col} = a.tid
            where t.{col} is null
        """
        cur.execute(sql, (ids,))
        rows = cur.fetchall()
        return [r[0] for r in rows]

def main():
    assert API_TOKEN, "Defina MOVIDESK_TOKEN no ambiente."
    assert NEON_DSN,  "Defina NEON_DSN no ambiente."

    # Janela dos últimos WINDOW_HRS
    end = now_utc()
    start = end - __import__("datetime").timedelta(hours=WINDOW_HRS)

    with psycopg2.connect(NEON_DSN) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Mantém apenas a ÚLTIMA execução em audit_recent_missing
            cur.execute(f"truncate table {TABLE_MISSING}")

            # Cria o run (missing_total inicia em 0)
            cur.execute(
                f"""
                insert into {TABLE_RUN}
                    (started_at, window_start, window_end, total_api, missing_total)
                values (now(), %s, %s, 0, 0)
                returning id
                """,
                (start, end),
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        # Busca IDs na API
        ids = movidesk_get_tickets(API_TOKEN, start, end)
        total_api = len(ids)

        # Atualiza total_api
        with conn.cursor() as cur:
            cur.execute(
                f"update {TABLE_RUN} set total_api = %s where id = %s",
                (total_api, run_id),
            )
        conn.commit()

        # Compara com as tabelas-alvo e popula missing
        total_missing = 0
        with conn.cursor() as cur:
            for table in TABLES_TO_CHECK:
                missing = compute_missing_for_table(conn, table, ids)
                if not missing:
                    continue
                rows = [(run_id, table.split(".",1)[1], t) for t in missing]
                execute_values(
                    cur,
                    f"insert into {TABLE_MISSING} (run_id, table_name, ticket_id) values %s",
                    rows,
                    page_size=1000,
                )
                total_missing += len(missing)

        # Atualiza missing_total do run
        with conn.cursor() as cur:
            cur.execute(
                f"update {TABLE_RUN} set missing_total = %s where id = %s",
                (total_missing, run_id),
            )
        conn.commit()

        print({
            "run_id": run_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "total_api": total_api,
            "missing_total": total_missing,
        })

if __name__ == "__main__":
    main()
