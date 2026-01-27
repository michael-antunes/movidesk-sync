import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_anchor_db_max_updated_at_api_candidates_paged_v3_logs_per_ticket_2026-01-27"


def env_str(k: str, default: Optional[str] = None) -> str:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {k}")
        return default
    return v.strip()


def env_int(k: str, default: int) -> int:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def env_float(k: str, default: float) -> float:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v.strip())
    except Exception:
        return default


def env_bool(k: str, default: bool = False) -> bool:
    v = os.getenv(k)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on", "sim")


def qident(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def qname(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def parse_dt(v: Any) -> Optional[dt.datetime]:
    if not v:
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        x = dt.datetime.fromisoformat(s)
        return x if x.tzinfo else x.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(str(v).strip())
        except Exception:
            return None


def to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[int] = []
        for x in v:
            xi = to_int(x)
            if xi is not None:
                out.append(xi)
        return out
    s = str(v).strip()
    if not s:
        return []
    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        xi = to_int(p)
        if xi is not None:
            out.append(xi)
    return out


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def odata_dt_variants(ts: dt.datetime) -> List[str]:
    z = ts.astimezone(dt.timezone.utc).replace(microsecond=0)
    iso = z.isoformat().replace("+00:00", "Z")
    return [iso, f"datetimeoffset'{iso}'", f"datetime'{iso}'"]


def normalize_list_response(data: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        v = data.get("value")
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return None


# -------------------------
# DB
# -------------------------
def ensure_mesclados_table(conn, schema: str, table: str) -> None:
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


def get_anchor_max_updated_at(conn, src_schema: str, src_table: str, src_date_col: str) -> dt.datetime:
    with conn.cursor() as cur:
        cur.execute(f"select max({qident(src_date_col)}) from {qname(src_schema, src_table)}")
        row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError(f"Não achei max({src_date_col}) em {src_schema}.{src_table}")
        max_ts = row[0]
        if isinstance(max_ts, dt.datetime) and max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=dt.timezone.utc)
        return max_ts


def upsert_mesclados(
    conn,
    schema: str,
    table: str,
    rows: List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]],
) -> int:
    if not rows:
        return 0
    sql = f"""
        insert into {qname(schema, table)} (ticket_id, merged_into_id, merged_at, raw_payload)
        values %s
        on conflict (ticket_id) do update set
          merged_into_id = excluded.merged_into_id,
          merged_at = coalesce(excluded.merged_at, {qname(schema, table)}.merged_at),
          raw_payload = excluded.raw_payload
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# -------------------------
# Movidesk API
# -------------------------
def movidesk_get_json(sess: requests.Session, url: str, params: Dict[str, Any], timeout: int) -> Tuple[Any, int, str]:
    r = sess.get(url, params=params, timeout=timeout)
    txt = r.text or ""
    if r.status_code != 200:
        return None, r.status_code, txt[:2000]
    try:
        return r.json(), 200, ""
    except Exception:
        return None, 200, txt[:2000]


def list_recent_tickets_paged_up_to_anchor(
    sess: requests.Session,
    base_url: str,
    token: str,
    anchor_ts: dt.datetime,
    page_size: int,
    pages: int,
    timeout: int,
    log: logging.Logger,
) -> Tuple[List[Tuple[int, Optional[dt.datetime]]], str, str]:
    endpoints = [
        f"{base_url.rstrip('/')}/tickets",
        f"{base_url.rstrip('/')}/tickets/past",
    ]
    id_fields = ["id", "ticketId", "ticket_id"]
    upd_fields = ["lastUpdate", "updatedAt", "updatedDate", "lastUpdateDate", "lastUpdated"]

    last_err = ""
    chosen = ""

    for url in endpoints:
        for idf in id_fields:
            for upf in upd_fields:
                for dt_expr in odata_dt_variants(anchor_ts):
                    all_items: List[Tuple[int, Optional[dt.datetime]]] = []
                    ok = True

                    for page in range(max(1, pages)):
                        skip = page * page_size
                        params = {
                            "token": token,
                            "$select": f"{idf},{upf}",
                            "$orderby": f"{upf} desc,{idf} desc",
                            "$top": str(page_size),
                            "$skip": str(skip),
                            "$filter": f"{upf} le {dt_expr}",
                        }

                        data, status, errtxt = movidesk_get_json(sess, url, params, timeout)
                        if status != 200:
                            ok = False
                            last_err = f"url={url} status={status} err={errtxt}"
                            break

                        items = normalize_list_response(data)
                        if items is None:
                            ok = False
                            last_err = f"url={url} status=200 but not list-like (type={type(data)})"
                            break

                        log.info("list_tickets page=%d skip=%d top=%d got=%d url=%s", page + 1, skip, page_size, len(items), url)

                        for it in items:
                            tid = to_int(it.get(idf))
                            if tid is None:
                                continue
                            upd = parse_dt(it.get(upf))
                            all_items.append((tid, upd))

                        if len(items) < page_size:
                            break

                    if ok:
                        chosen = f"url={url} id_field={idf} upd_field={upf} filter='{upf} le {dt_expr}' pages={pages} page_size={page_size}"
                        seen = set()
                        dedup: List[Tuple[int, Optional[dt.datetime]]] = []
                        for tid, upd in all_items:
                            if tid in seen:
                                continue
                            seen.add(tid)
                            dedup.append((tid, upd))
                        return dedup, chosen, last_err

    return [], chosen, last_err


def movidesk_get_merged_raw(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
    query_keys: List[str],
) -> Tuple[Any, Optional[int], str]:
    url = f"{base_url.rstrip('/')}/tickets/merged"
    last_status: Optional[int] = None
    last_key = ""

    for key in query_keys:
        last_key = key
        params = {"token": token, key: str(ticket_id)}
        for i in range(5):
            try:
                r = sess.get(url, params=params, timeout=timeout)
                last_status = r.status_code

                if r.status_code == 404:
                    break

                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 * (i + 1))
                    continue

                if r.status_code != 200:
                    return None, r.status_code, key

                try:
                    return r.json(), 200, key
                except Exception:
                    return None, 200, key
            except Exception:
                time.sleep(2 * (i + 1))

    return None, last_status, last_key


def extract_rows_from_merge_payload(
    queried_id: int,
    payload_any: Any,
) -> List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]]:
    if payload_any is None:
        return []

    out: List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = []

    def add_row(child_id: int, parent_id: int, merged_at: Optional[dt.datetime], raw_obj: Any) -> None:
        # filtro de relevância
        if int(child_id) != int(queried_id) and int(parent_id) != int(queried_id):
            return
        out.append((int(child_id), int(parent_id), merged_at, json_payload(raw_obj)))

    def handle_obj(obj: Dict[str, Any]) -> None:
        merged_at = parse_dt(
            obj.get("mergedDate")
            or obj.get("mergedAt")
            or obj.get("performedAt")
            or obj.get("date")
            or obj.get("createdAt")
            or obj.get("createdDate")
            or obj.get("lastUpdate")
            or obj.get("last_update")
        )

        # formato ticketId (pai) + mergedTicketId (filho)
        if obj.get("ticketId") is not None and (obj.get("mergedTicketId") is not None or obj.get("mergedTicketID") is not None):
            parent_id = to_int(obj.get("ticketId"))
            child_id = to_int(obj.get("mergedTicketId") or obj.get("mergedTicketID"))
            if parent_id is not None and child_id is not None:
                add_row(child_id, parent_id, merged_at, obj)
            return

        # formato mergedIntoId (pai)
        merged_into = (
            obj.get("mergedIntoId")
            or obj.get("mergedIntoTicketId")
            or obj.get("mainTicketId")
            or obj.get("mainTicketID")
            or obj.get("principalTicketId")
            or obj.get("principalId")
            or obj.get("mergedInto")
        )
        if merged_into is not None:
            parent_id = to_int(merged_into)
            child_guess = to_int(obj.get("ticketId")) or to_int(obj.get("id")) or int(queried_id)
            if parent_id is not None and child_guess is not None:
                add_row(int(child_guess), int(parent_id), merged_at, obj)
            return

        # formato lista de filhos no pai
        merged_ids = to_int_list(obj.get("mergedTicketsIds") or obj.get("mergedTicketsIDs") or obj.get("mergedTicketsIdsList"))
        if not merged_ids:
            mt = obj.get("mergedTickets") or obj.get("mergedTicketsList")
            if isinstance(mt, list):
                tmp: List[int] = []
                for it in mt:
                    if isinstance(it, dict):
                        tid = to_int(it.get("id") or it.get("ticketId") or it.get("ticketID") or it.get("ticket_id"))
                        if tid is not None:
                            tmp.append(tid)
                merged_ids = tmp

        if merged_ids:
            parent_guess = to_int(obj.get("ticketId") or obj.get("id") or obj.get("ticket_id")) or int(queried_id)
            for child_id in merged_ids:
                add_row(int(child_id), int(parent_guess), merged_at, obj)
            return

    if isinstance(payload_any, dict):
        handle_obj(payload_any)
    elif isinstance(payload_any, list):
        for it in payload_any:
            if isinstance(it, dict):
                handle_obj(it)

    dedup: Dict[int, Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = {}
    for r in out:
        dedup[int(r[0])] = r
    return list(dedup.values())


# -------------------------
# Main
# -------------------------
def main() -> None:
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync-tickets-merged")

    token = env_str("MOVIDESK_TOKEN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    neon_dsn = env_str("NEON_DSN")

    dst_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    dst_table = env_str("TABLE_NAME", "tickets_mesclados")

    src_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    src_table = env_str("SOURCE_TABLE", "tickets_suporte")
    src_date_col = env_str("SOURCE_DATE_COL", "updated_at")

    page_size = env_int("PAGE_SIZE", 50)
    pages = env_int("PAGES", 2)
    page_size = max(1, min(100, page_size))
    pages = max(1, min(10, pages))

    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    http_timeout = env_int("HTTP_TIMEOUT", 45)
    merged_query_keys = [k.strip() for k in env_str("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if k.strip()]

    log_every_ticket = env_bool("LOG_EVERY_TICKET", True)

    log.info("script_version=%s page_size=%d pages=%d rpm=%.2f log_every_ticket=%s",
             SCRIPT_VERSION, page_size, pages, rpm, log_every_ticket)

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    conn = psycopg2.connect(neon_dsn)
    conn.autocommit = False
    try:
        ensure_mesclados_table(conn, dst_schema, dst_table)
        conn.commit()

        anchor_ts = get_anchor_max_updated_at(conn, src_schema, src_table, src_date_col)
        conn.commit()

        log.info("anchor_ts_db_max=%s (%s.%s.%s)", anchor_ts.isoformat(), src_schema, src_table, src_date_col)

        candidates, chosen, last_err = list_recent_tickets_paged_up_to_anchor(
            sess=sess,
            base_url=base_url,
            token=token,
            anchor_ts=anchor_ts,
            page_size=page_size,
            pages=pages,
            timeout=http_timeout,
            log=log,
        )
        log.info("api_candidates=%d chosen=%s last_err=%s", len(candidates), chosen, (last_err or ""))

        if not candidates:
            log.info("done_no_candidates")
            return

        status_counts: Dict[int, int] = {}
        rows_map: Dict[int, Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = {}

        checked = 0
        found = 0

        total = len(candidates)

        for idx, (ticket_id, upd) in enumerate(candidates, start=1):
            checked += 1
            if log_every_ticket:
                log.info("checking_ticket %d/%d ticket_id=%s api_lastUpdate=%s", idx, total, ticket_id, upd.isoformat() if upd else None)

            payload, st, used_key = movidesk_get_merged_raw(
                sess=sess,
                base_url=base_url,
                token=token,
                ticket_id=int(ticket_id),
                timeout=http_timeout,
                query_keys=merged_query_keys,
            )
            if st is not None:
                status_counts[st] = status_counts.get(st, 0) + 1

            rows = extract_rows_from_merge_payload(int(ticket_id), payload)
            if log_every_ticket:
                log.info("merged_result ticket_id=%s status=%s key=%s rows=%d", ticket_id, st, used_key, len(rows))

            if rows:
                found += len(rows)
                for r in rows:
                    rows_map[int(r[0])] = r

            if throttle > 0:
                time.sleep(throttle)

        upserted = upsert_mesclados(conn, dst_schema, dst_table, list(rows_map.values()))
        conn.commit()

        log.info(
            "done checked=%d merges_found=%d upserted=%d statuses=%s",
            checked,
            found,
            upserted,
            json.dumps(status_counts, ensure_ascii=False),
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
