import os
import re
import time
import json
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")
BATCH = int(os.getenv("MERGED_BATCH", "400"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent": "movidesk-sync/merged"})


def md_get(path_or_full, params=None, ok_404=False):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN
    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or []
    if ok_404 and r.status_code == 404:
        return None
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or []
    r.raise_for_status()


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_resolvidos")
        cur.execute(
            """
            create table if not exists visualizacao_resolvidos.tickets_mesclados(
              ticket_id integer primary key,
              merged_into_id integer,
              merged_at timestamptz,
              situacao_mesclado text generated always as ('Sim') stored,
              raw_payload jsonb,
              imported_at timestamptz default now()
            )
            """
        )
        cur.execute(
            "create index if not exists ix_tk_merged_into on visualizacao_resolvidos.tickets_mesclados(merged_into_id)"
        )
    conn.commit()


def ensure_sync_control(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists visualizacao_resolvidos.sync_control(
              name text primary key,
              job_name text,
              last_update timestamptz,
              run_at timestamptz default now()
            )
            """
        )
        cur.execute(
            "alter table visualizacao_resolvidos.sync_control add column if not exists name text"
        )
        cur.execute(
            "alter table visualizacao_resolvidos.sync_control add column if not exists job_name text"
        )
        cur.execute(
            "alter table visualizacao_resolvidos.sync_control add column if not exists last_update timestamptz"
        )
        cur.execute(
            "alter table visualizacao_resolvidos.sync_control add column if not exists run_at timestamptz default now()"
        )
    conn.commit()


def upsert_rows(conn, rows):
    if not rows:
        return
    sql = """
    insert into visualizacao_resolvidos.tickets_mesclados
      (ticket_id, merged_into_id, merged_at, raw_payload)
    values %s
    on conflict (ticket_id) do update set
      merged_into_id = excluded.merged_into_id,
      merged_at      = coalesce(excluded.merged_at, visualizacao_resolvidos.tickets_mesclados.merged_at),
      raw_payload    = excluded.raw_payload,
      imported_at    = now()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()


def normalize_merged_response(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        v = raw.get("mergedTickets")
        if isinstance(v, list):
            return v
        for key in ("value", "items", "data", "tickets", "results"):
            v = raw.get(key)
            if isinstance(v, list):
                return v
        print("tickets_mesclados: resposta inesperada de /tickets/merged:", json.dumps(raw, ensure_ascii=False)[:400])
        return []
    print("tickets_mesclados: tipo inesperado em /tickets/merged:", type(raw).__name__)
    return []


def get_last_sync_update(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select max(last_update)
              from visualizacao_resolvidos.sync_control
             where job_name = %s
            """,
            ("tickets_merged",),
        )
        row = cur.fetchone()
        return row[0]


def register_sync_run(conn, last_update):
    if not last_update:
        return
    job = "tickets_merged"
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into visualizacao_resolvidos.sync_control(name, job_name, last_update, run_at)
            values (%s, %s, %s, now())
            on conflict (name) do update set
              job_name    = excluded.job_name,
              last_update = excluded.last_update,
              run_at      = now()
            """,
            (job, job, last_update),
        )
    conn.commit()


def fmt_dt_for_md(d):
    if not d:
        return None
    if isinstance(d, str):
        return d
    if not isinstance(d, datetime):
        return str(d)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def try_fetch_dedicated(conn):
    last_dt = get_last_sync_update(conn)
    now_dt = datetime.now(timezone.utc)
    base_params = {}
    s = fmt_dt_for_md(last_dt)
    if s:
        base_params["startDate"] = s
    base_params["endDate"] = fmt_dt_for_md(now_dt)
    page = 1
    total_pages = None
    total_inserted = 0
    processed = 0
    max_dt_val = None

    print(f"tickets_mesclados: chamada dedicada /tickets/merged base_params={base_params}")

    while True:
        params = dict(base_params)
        params["pageNumber"] = page
        try:
            raw = md_get("tickets/merged", params=params, ok_404=True)
        except requests.HTTPError as e:
            print(f"[WARN] dedicated endpoint not available ({e}). Using fallback by histories.")
            break
        except Exception as e:
            print(f"[WARN] erro ao chamar /tickets/merged: {e}. Usando fallback por histories.")
            break

        if raw is None:
            print("tickets_mesclados: endpoint /tickets/merged retornou 404.")
            break

        data = normalize_merged_response(raw)
        if not data:
            break

        if isinstance(raw, dict) and total_pages is None:
            pn = raw.get("pageNumber") or ""
            m = re.search(r"of\s+(\d+)", str(pn))
            if m:
                try:
                    total_pages = int(m.group(1))
                except Exception:
                    total_pages = None

        rows = []
        for it in data:
            if not isinstance(it, dict):
                continue
            principal = it.get("ticketId") or it.get("id")
            merged_ids_raw = it.get("mergedTicketsIds") or it.get("mergedTicketsId")
            dt_val = it.get("lastUpdate") or it.get("mergedDate") or it.get("performedAt") or it.get("date")

            try:
                principal = int(str(principal)) if principal is not None else None
            except Exception:
                principal = None

            if not principal or not merged_ids_raw:
                continue

            if isinstance(merged_ids_raw, (list, tuple, set)):
                ids_list = merged_ids_raw
            else:
                parts = re.split(r"[,\s;]+", str(merged_ids_raw))
                ids_list = [p for p in parts if p]

            for sid in ids_list:
                if processed >= BATCH:
                    break
                try:
                    src = int(str(sid))
                except Exception:
                    continue
                rows.append((src, principal, dt_val, json.dumps(it, ensure_ascii=False)))
                processed += 1
                if dt_val and (max_dt_val is None or str(dt_val) > str(max_dt_val)):
                    max_dt_val = dt_val
            if processed >= BATCH:
                break

        if rows:
            upsert_rows(conn, rows)
            total_inserted += len(rows)

        if processed >= BATCH:
            break
        if total_pages is not None and page >= total_pages:
            break

        page += 1
        time.sleep(THROTTLE)

    if max_dt_val:
        register_sync_run(conn, max_dt_val)
    else:
        register_sync_run(conn, now_dt)

    if total_inserted == 0:
        print("tickets_mesclados: nenhum novo registro via /tickets/merged.")
    else:
        print(f"tickets_mesclados: {total_inserted} registros inseridos via /tickets/merged.")
    return True


JUSTIF_RX = re.compile(
    r"(mescl|merge|unid|duplic|juntad|juntar|junc|anexad|anexar|vinculad|vincul|uni[ãa]o|unificar)",
    re.I,
)

TARGET_ID_RX = re.compile(
    r"(?:#|n[ºo]\s*|id\s*:?|ticket\s*:?|protocolo\s*:?)[^\d]*(\d{3,})",
    re.I,
)


def get_missing_candidate_ids_from_audit(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select a.ticket_id
              from visualizacao_resolvidos.audit_recent_missing a
         left join visualizacao_resolvidos.tickets_mesclados tm
                on tm.ticket_id = a.ticket_id
             where a.table_name = 'tickets_resolvidos'
               and tm.ticket_id is null
          group by a.ticket_id
          order by max(a.run_id) desc, a.ticket_id desc
             limit %s
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def fetch_histories_for(ids):
    if not ids:
        return []
    all_data = []
    for chunk in chunked(ids, 10):
        filtro = " or ".join([f"id eq {i}" for i in chunk])
        params = {
            "$select": "id",
            "$filter": filtro,
            "$expand": "statusHistories($select=status,justification,changedDate)",
        }
        try:
            data = md_get("tickets", params)
        except requests.HTTPError as e:
            print(f"[WARN] erro HTTP ao buscar histories para chunk {chunk}: {e}")
            continue
        except Exception as e:
            print(f"[WARN] erro inesperado ao buscar histories para chunk {chunk}: {e}")
            continue
        all_data.extend(data or [])
        time.sleep(THROTTLE)
    return all_data


def extract_merge_from_histories(item):
    tid = item.get("id")
    if tid is None:
        return None

    best_dt = None
    target = None

    for h in item.get("statusHistories") or []:
        status_txt = (h.get("status") or "")[:200]
        just = (h.get("justification") or "")[:400]

        has_merge_word = bool(JUSTIF_RX.search(just or "")) or bool(JUSTIF_RX.search(status_txt or ""))
        m = TARGET_ID_RX.search(just or "")

        if not has_merge_word and not m:
            continue

        if m:
            try:
                target = int(m.group(1))
            except Exception:
                pass

        dt = h.get("changedDate") or h.get("date")
        if dt and (best_dt is None or str(dt) > str(best_dt)):
            best_dt = dt

    if not best_dt and not target:
        return None

    try:
        tid_int = int(tid)
    except Exception:
        return None

    return tid_int, target, best_dt


def run_fallback_for_missing(conn):
    ids = get_missing_candidate_ids_from_audit(conn, BATCH)
    if not ids:
        print("tickets_mesclados: fallback(missing) não encontrou candidatos em audit_recent_missing.")
        return

    data = fetch_histories_for(ids)
    rows = []
    for it in data:
        got = extract_merge_from_histories(it)
        if not got:
            continue
        src, dst, dt = got
        rows.append((src, dst, dt, json.dumps(it, ensure_ascii=False)))

    if not rows:
        print(f"tickets_mesclados: fallback(missing) processou {len(ids)} tickets mas não identificou merges.")
        return

    upsert_rows(conn, rows)
    print(f"tickets_mesclados: {len(rows)} registros inseridos via fallback de statusHistories para missing.")


def main():
    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        ensure_sync_control(conn)
        try_fetch_dedicated(conn)
        try:
            run_fallback_for_missing(conn)
        except requests.HTTPError as e:
            print(f"[WARN] fallback(missing) por histories falhou: {e}")
        except Exception as e:
            print(f"[WARN] erro inesperado no fallback(missing): {e}")
        with conn.cursor() as cur:
            cur.execute("select count(*) from visualizacao_resolvidos.tickets_mesclados")
            total = cur.fetchone()[0]
        print(f"tickets_mesclados: sincronização concluída. Total na tabela: {total}.")


if __name__ == "__main__":
    main()
