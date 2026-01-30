import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests


def getenv_str(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return str(v)


def getenv_int(name: str, default: int, required: bool = False) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        if required:
            raise RuntimeError(f"Missing required env var: {name}")
        return int(default)
    return int(str(v).strip())


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def setup_logger() -> logging.Logger:
    level = getenv_str("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("sync_range_tickets_merged")


logger = setup_logger()


@dataclass
class Control:
    id_inicial: int
    id_final: int
    id_atual_merged: Optional[int]


@dataclass
class FetchResult:
    status_code: int
    data: Any
    url: str
    tried_params: List[Dict[str, Any]]


def set_session_timeouts(conn, statement_timeout_ms: int = 60000) -> None:
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = %s", (statement_timeout_ms,))


def ensure_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"create schema if not exists {qident(schema)}")
        cur.execute(
            f"""
            create table if not exists {qname(schema, table)} (
              ticket_id bigint primary key,
              merged_into_id bigint not null,
              merged_at timestamptz,
              raw_payload jsonb
            )
            """
        )
        cur.execute(f"create index if not exists ix_{table}_merged_into_id on {qname(schema, table)} (merged_into_id)")
        cur.execute(f"create index if not exists ix_{table}_merged_at on {qname(schema, table)} (merged_at)")


def read_control(conn, schema: str, table: str) -> Control:
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"""
                select id_inicial, id_final, id_atual_merged
                from {qname(schema, table)}
                where data_fim is null
                order by data_inicio desc nulls last
                limit 1
                """
            )
            row = cur.fetchone()
            if row:
                return Control(int(row[0]), int(row[1]), None if row[2] is None else int(row[2]))
        except Exception:
            conn.rollback()

        cur.execute(
            f"""
            select id_inicial, id_final, id_atual_merged
            from {qname(schema, table)}
            order by data_fim desc nulls last, data_inicio desc nulls last
            limit 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Control table {qname(schema, table)} is empty")
        return Control(int(row[0]), int(row[1]), None if row[2] is None else int(row[2]))


def update_last_processed(conn, schema: str, table: str, id_inicial: int, id_final: int, last_processed: int) -> None:
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"""
                update {qname(schema, table)}
                set id_atual_merged = %s
                where data_fim is null and id_inicial = %s and id_final = %s
                """,
                (last_processed, id_inicial, id_final),
            )
            if cur.rowcount == 1:
                return
        except Exception:
            conn.rollback()

        cur.execute(
            f"""
            update {qname(schema, table)}
            set id_atual_merged = %s
            where id_inicial = %s and id_final = %s
            """,
            (last_processed, id_inicial, id_final),
        )
        if cur.rowcount < 1:
            raise RuntimeError(
                f"update_last_processed updated {cur.rowcount} rows for {qname(schema, table)} (id_inicial={id_inicial}, id_final={id_final})"
            )


def commit_with_retry(conn, max_retries: int = 4) -> None:
    for attempt in range(max_retries):
        try:
            conn.commit()
            return
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = (2**attempt) * 0.25 + random.random() * 0.25
            logger.warning("commit failed (%s). retrying in %.2fs", type(e).__name__, wait)
            time.sleep(wait)


def fetch_merged_relations(
    session: requests.Session,
    base_url: str,
    endpoint: str,
    token: str,
    ticket_id: int,
    timeout_s: int,
) -> FetchResult:
    common = {"token": token}
    tries = [
        {**common, "ticketId": ticket_id},
        {**common, "id": ticket_id},
        {**common, "q": ticket_id},
    ]

    tried_params: List[Dict[str, Any]] = []
    url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")

    for p in tries:
        tried_params.append(dict(p))
        r = session.get(url, params=p, timeout=timeout_s)
        if r.status_code == 204:
            return FetchResult(r.status_code, None, r.url, tried_params)
        if r.status_code == 404:
            continue
        if r.status_code != 200:
            try:
                data = r.json()
            except Exception:
                data = r.text
            return FetchResult(r.status_code, data, r.url, tried_params)
        try:
            data = r.json()
        except Exception:
            data = r.text
        return FetchResult(r.status_code, data, r.url, tried_params)

    return FetchResult(404, None, url, tried_params)


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(str(v).strip())
        except Exception:
            return None


def _extract_pairs_from_dict(d: Dict[str, Any], queried_id: int) -> Tuple[str, Optional[int], List[int]]:
    keys_children = ["mergedTicketsIds", "mergedTicketIds", "mergedTickets", "mergedTicketsId"]
    child_ids: List[int] = []
    for k in keys_children:
        if k in d and d[k] is not None:
            v = d[k]
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        cid = _to_int(it.get("id") or it.get("ticketId") or it.get("mergedTicketId"))
                        if cid is not None:
                            child_ids.append(cid)
                    else:
                        cid = _to_int(it)
                        if cid is not None:
                            child_ids.append(cid)
            elif isinstance(v, dict):
                cid = _to_int(v.get("id") or v.get("ticketId") or v.get("mergedTicketId"))
                if cid is not None:
                    child_ids.append(cid)
            else:
                cid = _to_int(v)
                if cid is not None:
                    child_ids.append(cid)

    if child_ids:
        master = _to_int(d.get("ticketId") or d.get("id") or d.get("mergedIntoId") or d.get("mergedIntoTicketId")) or queried_id
        return ("master_list", master, sorted(set(child_ids)))

    merged_into = _to_int(d.get("mergedIntoId") or d.get("mergedIntoTicketId") or d.get("mergedInto"))
    merged_ticket = _to_int(d.get("mergedTicketId") or d.get("mergedTicket") or d.get("mergedTicketID"))
    if merged_into is not None and merged_ticket is not None:
        return ("pairs", None, [])

    if merged_into is not None:
        return ("child", merged_into, [queried_id])

    return ("none", None, [])


def detect_mode_and_relations(data: Any, queried_id: int) -> Tuple[str, Optional[int], List[int], List[Tuple[int, int, Optional[str], Any]]]:
    rows: List[Tuple[int, int, Optional[str], Any]] = []

    def add(child: int, master: int, merged_at: Optional[str], raw: Any) -> None:
        rows.append((int(child), int(master), merged_at, raw))

    if data is None:
        return ("none", None, [], [])

    if isinstance(data, dict):
        mode, master_or_none, children = _extract_pairs_from_dict(data, queried_id)

        if mode == "master_list":
            merged_at = data.get("mergedAt") or data.get("merged_at") or data.get("createdDate") or data.get("date")
            for c in children:
                add(c, master_or_none if master_or_none is not None else queried_id, merged_at, data)
            return ("master_list", master_or_none, children, rows)

        if mode == "child":
            merged_at = data.get("mergedAt") or data.get("merged_at") or data.get("createdDate") or data.get("date")
            master = master_or_none if master_or_none is not None else queried_id
            add(queried_id, master, merged_at, data)
            return ("child", master, [queried_id], rows)

        if mode == "pairs":
            merged_at = data.get("mergedAt") or data.get("merged_at") or data.get("createdDate") or data.get("date")
            master = _to_int(data.get("ticketId") or data.get("id") or data.get("mergedIntoId") or data.get("mergedIntoTicketId")) or queried_id
            child = _to_int(data.get("mergedTicketId") or data.get("mergedTicket")) or queried_id
            add(child, master, merged_at, data)
            return ("master_list", master, [child], rows)

        return ("none", None, [], [])

    if isinstance(data, list):
        pairs: List[Tuple[int, int, Optional[str], Any]] = []
        masters: List[int] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            merged_at = it.get("mergedAt") or it.get("merged_at") or it.get("createdDate") or it.get("date")
            master = _to_int(it.get("ticketId") or it.get("id") or it.get("mergedIntoId") or it.get("mergedIntoTicketId"))
            child = _to_int(it.get("mergedTicketId") or it.get("mergedTicket") or it.get("ticketMergedId"))
            if master is not None and child is not None:
                pairs.append((child, master, merged_at, it))
                masters.append(master)

        if pairs:
            masters_u = sorted(set(masters))
            if len(masters_u) == 1:
                master = masters_u[0]
                child_ids = sorted({p[0] for p in pairs})
                for child, master_, merged_at, raw in pairs:
                    add(child, master_, merged_at, raw)
                return ("master_list", master, child_ids, rows)
            for child, master_, merged_at, raw in pairs:
                add(child, master_, merged_at, raw)
            return ("pairs", None, [], rows)

        return ("none", None, [], [])

    return ("none", None, [], [])


def upsert_mesclados(conn, schema: str, table: str, rows: Sequence[Tuple[int, int, Optional[str], Any]]) -> int:
    if not rows:
        return 0
    values = []
    for child, master, merged_at, raw in rows:
        values.append((int(child), int(master), merged_at, psycopg2.extras.Json(raw, dumps=json.dumps)))
    sql = f"""
        insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
        values %s
        on conflict (ticket_id) do update
        set merged_into_id = excluded.merged_into_id,
            merged_at = excluded.merged_at,
            raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=200)
    return len(values)


def delete_stale_children_for_master(conn, schema: str, table: str, master_id: int, keep_children: Sequence[int]) -> int:
    keep = list({int(x) for x in keep_children})
    with conn.cursor() as cur:
        if keep:
            cur.execute(
                f"delete from {qname(schema, table)} where merged_into_id = %s and ticket_id <> all(%s)",
                (int(master_id), keep),
            )
        else:
            cur.execute(
                f"delete from {qname(schema, table)} where merged_into_id = %s",
                (int(master_id),),
            )
        return int(cur.rowcount)


def delete_by_ticket_id(conn, schema: str, table: str, ticket_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(f"delete from {qname(schema, table)} where ticket_id = %s", (int(ticket_id),))
        return int(cur.rowcount)


def delete_from_other_tables(
    conn,
    schema_resolvidos: str,
    resolved_detail_table: str,
    schema_atual: str,
    open_table: str,
    ticket_ids: Sequence[int],
) -> int:
    ids = list({int(x) for x in ticket_ids if x is not None})
    if not ids:
        return 0
    total = 0
    with conn.cursor() as cur:
        try:
            cur.execute(f"delete from {qname(schema_resolvidos, resolved_detail_table)} where ticket_id = any(%s)", (ids,))
            total += int(cur.rowcount)
        except Exception:
            conn.rollback()
        try:
            cur.execute(f"delete from {qname(schema_atual, open_table)} where ticket_id = any(%s)", (ids,))
            total += int(cur.rowcount)
        except Exception:
            conn.rollback()
    return total


def build_batch(next_id: int, id_final: int, batch_size: int) -> List[int]:
    if next_id < id_final:
        return []
    end_id = max(id_final, next_id - batch_size + 1)
    return list(range(next_id, end_id - 1, -1))


def main() -> int:
    script_version = getenv_str("SCRIPT_VERSION", "tickets_merged_range_v5")
    db_dsn = getenv_str("NEON_DSN", required=True)
    base_url = getenv_str("MOVIDESK_API_BASE", required=True)
    endpoint = getenv_str("MOVIDESK_MERGED_ENDPOINT", "/tickets/merged")
    token = getenv_str("MOVIDESK_TOKEN", required=True)

    schema_resolvidos = getenv_str("DB_SCHEMA_RESOLVIDOS", "visualizacao_resolvidos")
    merged_table = getenv_str("DB_TABLE_MESCLADOS", "tickets_mesclados")
    resolved_detail_table = getenv_str("DB_TABLE_RESOLVIDOS_DETAIL", "tickets_resolvidos_detail")

    schema_atual = getenv_str("DB_SCHEMA_ATUAL", "visualizacao_atual")
    open_table = getenv_str("DB_TABLE_ABERTOS", "tickets_abertos")

    control_schema = getenv_str("DB_SCHEMA_RESOLVIDOS", "visualizacao_resolvidos")
    control_table = getenv_str("DB_TABLE_RANGE_SCAN_CONTROL", "range_scan_control")

    batch_size = getenv_int("BATCH_SIZE", 10)
    timeout_s = getenv_int("HTTP_TIMEOUT_S", 40)
    statement_timeout_ms = getenv_int("STATEMENT_TIMEOUT_MS", 60000)

    logger.info("script_version=%s", script_version)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    conn = psycopg2.connect(db_dsn)
    conn.autocommit = False
    set_session_timeouts(conn, statement_timeout_ms=statement_timeout_ms)
    ensure_table(conn, schema_resolvidos, merged_table)
    commit_with_retry(conn)

    control = read_control(conn, control_schema, control_table)
    id_inicial, id_final, last_processed = control.id_inicial, control.id_final, control.id_atual_merged

    if id_inicial < id_final:
        logger.warning("control has id_inicial < id_final, swapping (id_inicial=%s, id_final=%s)", id_inicial, id_final)
        id_inicial, id_final = id_final, id_inicial

    if last_processed is None:
        next_id = id_inicial
    else:
        next_id = int(last_processed) - 1

    if next_id > id_inicial:
        next_id = id_inicial
    if next_id < id_final:
        logger.info("nothing to do: next_id=%s < id_final=%s", next_id, id_final)
        return 0

    logger.info("begin id_inicial=%s id_final=%s last_processed=%s next_id=%s batch=%s", id_inicial, id_final, last_processed, next_id, batch_size)

    checked_total = 0
    upsert_total = 0
    deleted_total = 0
    deleted_other_total = 0
    api_404 = 0
    api_err = 0

    while True:
        ids = build_batch(next_id, id_final, batch_size)
        if not ids:
            break

        for ticket_id in ids:
            checked_total += 1
            fr = fetch_merged_relations(session, base_url, endpoint, token, ticket_id, timeout_s=timeout_s)

            if fr.status_code == 404:
                api_404 += 1
                del_ct = delete_by_ticket_id(conn, schema_resolvidos, merged_table, ticket_id)
                deleted_total += del_ct
                update_last_processed(conn, control_schema, control_table, control.id_inicial, control.id_final, ticket_id)
                commit_with_retry(conn)
                logger.info("ticket=%s api=404 delete_mesclados=%s set_last_processed=%s", ticket_id, del_ct, ticket_id)
                next_id = ticket_id - 1
                continue

            if fr.status_code != 200:
                api_err += 1
                logger.error("ticket=%s api_status=%s url=%s data=%s", ticket_id, fr.status_code, fr.url, str(fr.data)[:400])
                conn.rollback()
                continue

            mode, master_id, child_ids, rows = detect_mode_and_relations(fr.data, ticket_id)

            up_ct = 0
            del_ct = 0
            del_other = 0

            if rows:
                up_ct = upsert_mesclados(conn, schema_resolvidos, merged_table, rows)
                upsert_total += up_ct
                if mode == "master_list" and master_id is not None:
                    del_ct = delete_stale_children_for_master(conn, schema_resolvidos, merged_table, master_id, child_ids)
                    deleted_total += del_ct
                affected_children = child_ids if child_ids else [ticket_id]
                del_other = delete_from_other_tables(conn, schema_resolvidos, resolved_detail_table, schema_atual, open_table, affected_children)
                deleted_other_total += del_other
            else:
                del_ct = delete_by_ticket_id(conn, schema_resolvidos, merged_table, ticket_id)
                deleted_total += del_ct

            update_last_processed(conn, control_schema, control_table, control.id_inicial, control.id_final, ticket_id)
            commit_with_retry(conn)

            if mode == "master_list" and master_id is not None:
                logger.info(
                    "ticket=%s mode=master_list master=%s children=%s upsert=%s delete_stale=%s delete_other=%s",
                    ticket_id,
                    master_id,
                    len(child_ids),
                    up_ct,
                    del_ct,
                    del_other,
                )
            elif mode == "child" and master_id is not None:
                logger.info(
                    "ticket=%s mode=child merged_into=%s upsert=%s delete_other=%s delete_mesclados=%s",
                    ticket_id,
                    master_id,
                    up_ct,
                    del_other,
                    del_ct,
                )
            elif mode == "pairs":
                logger.info(
                    "ticket=%s mode=pairs upsert=%s delete_other=%s delete_mesclados=%s",
                    ticket_id,
                    up_ct,
                    del_other,
                    del_ct,
                )
            else:
                logger.info(
                    "ticket=%s mode=none upsert=%s delete_mesclados=%s",
                    ticket_id,
                    up_ct,
                    del_ct,
                )

            next_id = ticket_id - 1

        logger.info(
            "progress next_id=%s checked=%s upsert=%s deleted=%s deleted_other=%s api404=%s api_err=%s",
            next_id,
            checked_total,
            upsert_total,
            deleted_total,
            deleted_other_total,
            api_404,
            api_err,
        )

    logger.info(
        "done checked=%s upsert=%s deleted=%s deleted_other=%s api404=%s api_err=%s",
        checked_total,
        upsert_total,
        deleted_total,
        deleted_other_total,
        api_404,
        api_err,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
