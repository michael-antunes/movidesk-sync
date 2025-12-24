import os
import time
import psycopg2
from psycopg2.extras import execute_values
import requests

API = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

SCHEMA = "visualizacao_resolvidos"
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "20"))
TOP = int(os.getenv("MOVIDESK_TOP", "1000"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

def conn():
    return psycopg2.connect(DSN)

def api_list_ids(endpoint, low_id, high_id):
    url = f"{API}/{endpoint}"
    params = {
        "token": TOKEN,
        "$select": "id,baseStatus",
        "$filter": f"id ge {int(low_id)} and id le {int(high_id)} and (baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')",
        "$orderby": "id",
        "$top": int(TOP),
        "includeDeletedItems": "true",
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
        f"select ticket_id from {SCHEMA}.tickets_resolvidos_detail where ticket_id = any(%s)",
        (list(ids),),
    )
    return {int(r[0]) for r in cur.fetchall()}

def ids_in_abertos(cur, ids):
    if not ids:
        return set()
    cur.execute(
        "select ticket_id from visualizacao_atual.tickets_abertos where ticket_id = any(%s)",
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
        f"select ticket_id from {SCHEMA}.audit_recent_missing where table_name=%s and ticket_id = any(%s)",
        (TABLE_NAME, list(ids)),
    )
    return {int(r[0]) for r in cur.fetchall()}

def get_control(cur):
    cur.execute(
        f"select id_inicial, id_final, id_atual from {SCHEMA}.range_scan_control limit 1"
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

def create_run(cur):
    cur.execute(f"insert into {SCHEMA}.audit_recent_run default values returning id")
    return int(cur.fetchone()[0])

def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        f"""
        insert into {SCHEMA}.audit_recent_missing(run_id, table_name, ticket_id, first_seen, last_seen, attempts)
        values %s
        on conflict (table_name, ticket_id) do update
          set last_seen = now()
        """,
        [(int(run_id), TABLE_NAME, int(tid)) for tid in ids],
        template="(%s,%s,%s,now(),now(),0)",
        page_size=1000,
    )
    return len(ids)

def main():
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

        for _ in range(int(ITERATIONS)):
            high_id = cursor
            low_id = max(int(id_final), cursor - int(BATCH_SIZE) + 1)

            api_ids = set(api_list_ids("tickets", low_id, high_id))
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
            c.commit()

            next_cursor = low_id - 1
            if next_cursor < int(id_final):
                set_id_atual(cur, None)
                c.commit()
                cursor = None
                break

            cursor = int(next_cursor)
            set_id_atual(cur, cursor)
            c.commit()

            if THROTTLE > 0:
                time.sleep(float(THROTTLE))

        done = 1 if cursor is None else 0
        cnow = "null" if cursor is None else str(cursor)
        print(
            f"done={done} ranges={total_ranges} api_ids={total_api_ids} excluded={total_excluded} inserted_missing={total_insert} cursor_now={cnow} id_inicial={id_inicial} id_final={id_final}"
        )

if __name__ == "__main__":
    main()
