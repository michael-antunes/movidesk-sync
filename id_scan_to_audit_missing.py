import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
import requests

API = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "20"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")


def to_utc(d):
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def conn():
    return psycopg2.connect(DSN)


def ensure_tables(cur):
    cur.execute(
        f"""
        create table if not exists {SCHEMA}.audit_recent_run(
          id bigserial primary key,
          created_at timestamptz not null default now(),
          table_name text not null,
          range_count int not null default 0,
          api_ids int not null default 0,
          inserted_missing int not null default 0
        )
        """
    )
    cur.execute(
        f"""
        create table if not exists {SCHEMA}.audit_recent_missing(
          run_id bigint not null,
          table_name text not null,
          ticket_id bigint not null,
          created_at timestamptz not null default now()
        )
        """
    )
    cur.execute(
        f"""
        create unique index if not exists audit_recent_missing_uniq
        on {SCHEMA}.audit_recent_missing(table_name, ticket_id)
        """
    )


def api_list_ids(endpoint, low_id, high_id):
    url = f"{API}/{endpoint}"
    params = {
        "token": TOKEN,
        "$select": "id",
        "$filter": f"id ge {low_id} and id le {high_id}",
        "$orderby": "id",
        "$top": TOP,
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:500]}")
    data = r.json() or []
    out = []
    for x in data:
        try:
            out.append(int(x.get("id")))
        except Exception:
            pass
    return out


def ids_in_detail(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.tickets_resolvidos_detail
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
        select ticket_id
        from visualizacao_atual.tickets_abertos
        where ticket_id = any(%s)
        """,
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}


def ids_in_mesclados(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id as id
        from {SCHEMA}.tickets_mesclados
        where ticket_id = any(%s)
        union
        select merged_into_id as id
        from {SCHEMA}.tickets_mesclados
        where merged_into_id = any(%s)
        """,
        (list(ids), list(ids)),
    )
    out = set()
    for (v,) in cur.fetchall():
        if v is not None:
            out.add(int(v))
    return out


def ids_in_missing(cur, ids):
    if not ids:
        return set()
    cur.execute(
        f"""
        select ticket_id
        from {SCHEMA}.audit_recent_missing
        where table_name = %s and ticket_id = any(%s)
        """,
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}


def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    rows = [(run_id, TABLE_NAME, int(t)) for t in ids]
    execute_values(
        cur,
        f"""
        insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        rows,
        page_size=1000,
    )
    return len(ids)


def create_run(cur):
    cur.execute(
        f"""
        insert into {SCHEMA}.audit_recent_run(table_name, range_count, api_ids, inserted_missing)
        values (%s, 0, 0, 0)
        returning id
        """,
        (TABLE_NAME,),
    )
    return cur.fetchone()[0]


def update_run(cur, run_id, api_ids, inserted_missing, range_count):
    cur.execute(
        f"""
        update {SCHEMA}.audit_recent_run
           set api_ids = %s,
               inserted_missing = %s,
               range_count = %s
         where id = %s
        """,
        (int(api_ids), int(inserted_missing), int(range_count), int(run_id)),
    )


def get_control(cur):
    cur.execute(
        f"""
        select id_inicial, id_final, id_atual
        from {SCHEMA}.range_scan_control
        limit 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def set_id_atual(cur, v):
    cur.execute(
        f"update {SCHEMA}.range_scan_control set id_atual=%s",
        (None if v is None else int(v),),
    )


def main():
    with conn() as c, c.cursor() as cur:
        ensure_tables(cur)
        c.commit()

    with conn() as c, c.cursor() as cur:
        id_inicial, id_final, id_atual = get_control(cur)
        if id_inicial is None or id_final is None:
            print("done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null")
            return

        cursor = int(id_atual) if id_atual is not None else int(id_inicial)
        if cursor < int(id_final):
            print("done=1 ranges=0 api_ids=0 excluded=0 inserted_missing=0 cursor_now=null")
            return

        run_id = create_run(cur)
        c.commit()

        total_ranges = 0
        total_api_ids = 0
        total_excluded = 0
        total_insert = 0

        for _ in range(ITERATIONS):
            if cursor is None:
                break

            high_id = cursor
            low_id = max(int(id_final), cursor - BATCH_SIZE + 1)

            api_ids = set()
            api_ids |= set(api_list_ids("tickets", low_id, high_id))
            api_ids |= set(api_list_ids("tickets/past", low_id, high_id))

            total_ranges += 1
            total_api_ids += len(api_ids)

            in_detail = ids_in_detail(cur, api_ids)
            in_abertos = ids_in_abertos(cur, api_ids)
            in_mesclados = ids_in_mesclados(cur, api_ids)
            in_missing = ids_in_missing(cur, api_ids)

            excluded = in_detail | in_abertos | in_mesclados | in_missing
            total_excluded += len(excluded)

            candidates = sorted(list(api_ids - excluded))
            total_insert += insert_missing(cur, run_id, candidates)

            next_cursor = low_id - 1
            if next_cursor < int(id_final):
                cursor = None
                set_id_atual(cur, None)
                c.commit()
                break

            cursor = int(next_cursor)
            set_id_atual(cur, cursor)
            c.commit()

            if THROTTLE > 0:
                time.sleep(THROTTLE)

        update_run(cur, run_id, total_api_ids, total_insert, total_ranges)
        c.commit()

        done = 1 if cursor is None else 0
        cnow = "null" if cursor is None else str(cursor)
        print(
            f"done={done} ranges={total_ranges} api_ids={total_api_ids} excluded={total_excluded} inserted_missing={total_insert} cursor_now={cnow} id_inicial={id_inicial} id_final={id_final}"
        )


if __name__ == "__main__":
    main()
