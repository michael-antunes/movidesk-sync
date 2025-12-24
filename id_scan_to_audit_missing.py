import os
import time
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import execute_values


DSN = os.getenv("NEON_DSN")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "1"))
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

API_BASE = "https://api.movidesk.com/public/v1"
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
CONNECT_TIMEOUT = int(os.getenv("MOVIDESK_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("MOVIDESK_READ_TIMEOUT", "60"))

BASE_STATUSES = ("Resolved", "Closed", "Canceled")
SELECT_LIST = "id,baseStatus,lastUpdate,isDeleted"


if not DSN:
    raise RuntimeError("NEON_DSN não definido")
if not TOKEN:
    raise RuntimeError("MOVIDESK_TOKEN não definido")


def ensure_control(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.range_scan_control(
          data_inicio timestamptz,
          data_fim timestamptz,
          ultima_data_validada timestamptz,
          id_inicial bigint,
          id_final bigint,
          id_atual bigint
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists data_inicio timestamptz")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists data_fim timestamptz")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists ultima_data_validada timestamptz")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_inicial bigint")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_final bigint")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_atual bigint")

    cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
    if cur.fetchone()[0] != 0:
        return

    cur.execute("select min(ticket_id), max(ticket_id) from visualizacao_resolvidos.tickets_resolvidos")
    min_id, max_id = cur.fetchone()

    if min_id is None or max_id is None:
        cur.execute("select min(ticket_id), max(ticket_id) from dados_gerais.tickets_suporte")
        min2, max2 = cur.fetchone()
        if min_id is None:
            min_id = min2
        if max_id is None:
            max_id = max2

    if min_id is None or max_id is None:
        min_id = 1
        max_id = 1

    cur.execute(
        """
        insert into visualizacao_resolvidos.range_scan_control
          (data_inicio, data_fim, ultima_data_validada, id_inicial, id_final, id_atual)
        values (now(), now(), now(), %s, %s, %s)
        """,
        (int(max_id), int(min_id), int(max_id)),
    )


def ensure_run_and_missing(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          window_start timestamptz not null,
          window_end timestamptz not null,
          total_api int not null,
          missing_total int not null,
          notes text
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_start timestamptz")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_end timestamptz")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists total_api int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists missing_total int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists notes text")

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint not null references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists run_id bigint")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists table_name text")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists ticket_id integer")

    cur.execute(
        """
        create unique index if not exists audit_recent_missing_uniq
        on visualizacao_resolvidos.audit_recent_missing(table_name, ticket_id)
        """
    )


def create_run(cur, notes):
    cur.execute(
        """
        insert into visualizacao_resolvidos.audit_recent_run(window_start, window_end, total_api, missing_total, notes)
        values (now(), now(), 0, 0, %s)
        returning id
        """,
        (notes,),
    )
    return int(cur.fetchone()[0])


def fetch_control(cur):
    cur.execute(
        """
        select id_inicial, id_final, id_atual
        from visualizacao_resolvidos.range_scan_control
        limit 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def update_control(cur, id_atual):
    cur.execute(
        """
        update visualizacao_resolvidos.range_scan_control
           set id_atual = %s,
               ultima_data_validada = now()
        """,
        (int(id_atual),),
    )


def http_get_json(url, params):
    q = urlencode(params)
    full = f"{url}?{q}"
    last = None
    for i in range(1, ATTEMPTS + 1):
        try:
            req = urllib.request.Request(full, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=CONNECT_TIMEOUT + READ_TIMEOUT) as resp:
                data = resp.read()
                if THROTTLE > 0:
                    time.sleep(THROTTLE)
                return resp.getcode(), json.loads(data.decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as e:
            code = e.code
            body = e.read()
            try:
                payload = json.loads(body.decode("utf-8", errors="replace")) if body else None
            except Exception:
                payload = None
            if code in (429, 500, 502, 503, 504):
                time.sleep(min(10, 2 ** (i - 1)))
                last = (code, payload)
                continue
            if THROTTLE > 0:
                time.sleep(THROTTLE)
            return code, payload
        except Exception as e:
            last = e
            time.sleep(min(10, 2 ** (i - 1)))
    raise last if last else RuntimeError("HTTP failure")


def api_list_ids(endpoint, low_id, high_id):
    flt = (
        f"id ge {int(low_id)} and id le {int(high_id)} and "
        f"(baseStatus eq '{BASE_STATUSES[0]}' or baseStatus eq '{BASE_STATUSES[1]}' or baseStatus eq '{BASE_STATUSES[2]}')"
    )
    ids = set()
    skip = 0
    while True:
        params = {
            "token": TOKEN,
            "includeDeletedItems": "true",
            "$filter": flt,
            "$select": SELECT_LIST,
            "$top": str(TOP),
            "$skip": str(skip),
            "$orderby": "id",
        }
        code, payload = http_get_json(f"{API_BASE}/{endpoint}", params)
        if code != 200 or not isinstance(payload, list):
            break
        if not payload:
            break
        for x in payload:
            if isinstance(x, dict) and x.get("id") is not None:
                ids.add(int(x["id"]))
        if len(payload) < TOP:
            break
        skip += TOP
    return ids


def ids_in_detail(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_resolvidos.tickets_resolvidos_detail
        where ticket_id = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_abertos(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id::bigint
        from visualizacao_atual.tickets_abertos
        where ticket_id::bigint = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_mesclados(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_resolvidos.tickets_mesclados
        where ticket_id = any(%s) or merged_into_id = any(%s)
        """,
        (list(ids), list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_missing(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_resolvidos.audit_recent_missing
        where table_name = %s and ticket_id = any(%s)
        """,
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        """
        insert into visualizacao_resolvidos.audit_recent_missing(run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        [(int(run_id), TABLE_NAME, int(t)) for t in ids],
    )
    return len(ids)


def main():
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        ensure_control(cur)
        ensure_run_and_missing(cur)
        run_id = create_run(cur, "id-scan-api: valida existencia na API (/tickets + /tickets/past) e insere faltantes")
        c.commit()

        total_api_ids = 0
        total_missing = 0

        id_inicial, id_final, id_atual = fetch_control(cur)
        if id_inicial is None or id_final is None:
            return

        if id_atual is None:
            id_atual = id_inicial
            update_control(cur, id_atual)
            c.commit()

        for _ in range(max(1, ITERATIONS)):
            if id_atual < id_final:
                break

            start_id = int(id_atual)
            end_id = int(max(id_final, id_atual - BATCH_SIZE + 1))

            api_ids = set()
            api_ids |= api_list_ids("tickets", end_id, start_id)
            api_ids |= api_list_ids("tickets/past", end_id, start_id)

            total_api_ids += len(api_ids)

            in_detail = ids_in_detail(cur, api_ids)
            in_abertos = ids_in_abertos(cur, api_ids)
            in_mesclados = ids_in_mesclados(cur, api_ids)
            in_missing = ids_in_missing(cur, api_ids)

            candidates = sorted(list(api_ids - in_detail - in_abertos - in_mesclados - in_missing))
            total_missing += insert_missing(cur, run_id, candidates)

            next_id = end_id - 1
            id_atual = next_id
            update_control(cur, id_atual)
            c.commit()

        cur.execute(
            """
            update visualizacao_resolvidos.audit_recent_run
               set window_end = now(),
                   total_api = %s,
                   missing_total = %s
             where id = %s
            """,
            (int(total_api_ids), int(total_missing), int(run_id)),
        )
        c.commit()


if __name__ == "__main__":
    main()
