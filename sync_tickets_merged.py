import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import psycopg2
import psycopg2.extras


SCRIPT_VERSION = "sync_tickets_merged_anchor_db_max_updated_at_candidates_api_top50_v1_2026-01-27"


# -------------------------
# Env helpers
# -------------------------
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


def to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[int] = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    s = str(v).strip()
    if not s:
        return []
    s = s.replace("[", "").replace("]", "")
    parts = [p.strip() for p in s.replace(",", ";").split(";") if p.strip()]
    out: List[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except Exception:
            pass
    return out


def json_payload(x: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(x, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def odata_dt_variants(ts: dt.datetime) -> List[str]:
    # formatos comuns que às vezes o OData aceita
    z = ts.astimezone(dt.timezone.utc).replace(microsecond=0)
    iso = z.isoformat().replace("+00:00", "Z")
    return [
        iso,  # 2026-01-27T12:34:56Z
        f"datetimeoffset'{iso}'",
        f"datetime'{iso}'",
    ]


def normalize_list_response(data: Any) -> Optional[List[Dict[str, Any]]]:
    """
    Algumas APIs retornam lista direta; outras retornam {"value": [...]}
    """
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


def pick_row(
    old: Optional[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]],
    new: Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json],
) -> Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]:
    if old is None:
        return new
    old_at = old[2]
    new_at = new[2]
    if old_at is None and new_at is not None:
        return new
    if old_at is not None and new_at is not None and new_at > old_at:
        return new
    return old


# -------------------------
# Movidesk API
# -------------------------
def movidesk_get_json(
    sess: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int,
) -> Tuple[Any, int, str]:
    r = sess.get(url, params=params, timeout=timeout)
    txt = r.text or ""
    if r.status_code != 200:
        return None, r.status_code, txt[:2000]
    try:
        return r.json(), 200, ""
    except Exception:
        return None, 200, txt[:2000]


def list_recent_tickets_from_api_up_to_anchor(
    sess: requests.Session,
    base_url: str,
    token: str,
    anchor_ts: dt.datetime,
    limit: int,
    timeout: int,
    log: logging.Logger,
) -> Tuple[List[Tuple[int, Optional[dt.datetime]]], str, str]:
    """
    Busca TOP N tickets mais recentes na API, com lastUpdate <= anchor_ts.
    Tenta /tickets e (fallback) /tickets/past.

    Retorna: [(ticket_id, lastUpdate)], chosen, last_err
    """
    endpoints = [
        f"{base_url.rstrip('/')}/tickets",
        f"{base_url.rstrip('/')}/tickets/past",
    ]

    id_fields = ["id", "ticketId", "ticket_id"]
    upd_fields = ["lastUpdate", "lastupdate", "updatedAt", "updatedDate", "lastUpdateDate", "lastUpdated"]

    last_err = ""
    chosen = ""

    for url in endpoints:
        for idf in id_fields:
            for upf in upd_fields:
                for dt_expr in odata_dt_variants(anchor_ts):
                    params = {
                        "token": token,
                        "$select": f"{idf},{upf}",
                        "$orderby": f"{upf} desc,{idf} desc",
                        "$top": str(min(100, max(1, limit))),
                        "$skip": "0",
                        "$filter": f"{upf} le {dt_expr}",
                    }

                    data, status, errtxt = movidesk_get_json(sess, url, params, timeout)
                    if status != 200:
                        last_err = f"url={url} status={status} err={errtxt}"
                        continue

                    items = normalize_list_response(data)
                    if items is None:
                        last_err = f"url={url} status=200 but not list-like (type={type(data)})"
                        continue

                    out: List[Tuple[int, Optional[dt.datetime]]] = []
                    for it in items:
                        tid_raw = it.get(idf)
                        try:
                            tid = int(tid_raw)
                        except Exception:
                            continue
                        upd = parse_dt(it.get(upf))
                        out.append((tid, upd))

                    chosen = f"url={url} id_field={idf} upd_field={upf} filter='{upf} le {dt_expr}'"
                    return out[:limit], chosen, last_err

        # fallback ultra-safe: sem filter (se OData filter der ruim)
        # ainda assim respeita "mais recentes"
        for idf in id_fields:
            for upf in upd_fields:
                params = {
                    "token": token,
                    "$select": f"{idf},{upf}",
                    "$orderby": f"{upf} desc,{idf} desc",
                    "$top": str(min(100, max(1, limit))),
                    "$skip": "0",
                }
                data, status, errtxt = movidesk_get_json(sess, url, params, timeout)
                if status != 200:
                    last_err = f"url={url} status={status} err={errtxt}"
                    continue
                items = normalize_list_response(data)
                if items is None:
                    last_err = f"url={url} status=200 but not list-like (type={type(data)})"
                    continue
                out: List[Tuple[int, Optional[dt.datetime]]] = []
                for it in items:
                    tid_raw = it.get(idf)
                    try:
                        tid = int(tid_raw)
                    except Exception:
                        continue
                    upd = parse_dt(it.get(upf))
                    out.append((tid, upd))
                chosen = f"url={url} id_field={idf} upd_field={upf} NO_FILTER"
                return out[:limit], chosen, last_err

    return [], chosen, last_err


def movidesk_get_merged(
    sess: requests.Session,
    base_url: str,
    token: str,
    ticket_id: int,
    timeout: int,
    query_keys: List[str],
) -> Tuple[Any, Optional[int]]:
    """
    Chama /tickets/merged tentando query param:
      - ticketId
      - id
      - q
    """
    url = f"{base_url.rstrip('/')}/tickets/merged"
    last_status: Optional[int] = None

    for key in query_keys:
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
                    return None, r.status_code

                try:
                    return r.json(), 200
                except Exception:
                    return None, 200
            except Exception:
                time.sleep(2 * (i + 1))

    return None, last_status


def extract_merge_rows(
    queried_id: int,
    raw: Dict[str, Any],
) -> List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]]:
    """
    Saída sempre no formato:
      (ticket_id_filho, merged_into_id_pai, merged_at, raw_payload)
    """
    payload = json_payload(raw)

    principal_id = raw.get("ticketId") or raw.get("id") or raw.get("ticket_id")
    try:
        principal_id_int = int(principal_id) if principal_id is not None else int(queried_id)
    except Exception:
        principal_id_int = int(queried_id)

    merged_at = parse_dt(
        raw.get("mergedDate")
        or raw.get("mergedAt")
        or raw.get("performedAt")
        or raw.get("date")
        or raw.get("lastUpdate")
        or raw.get("last_update")
    )

    # Caso 1: consultou o "pai" e veio lista de filhos mesclados
    merged_ids = to_int_list(raw.get("mergedTicketsIds") or raw.get("mergedTicketsIDs") or raw.get("mergedTicketsIdsList"))
    if not merged_ids:
        mt = raw.get("mergedTickets") or raw.get("mergedTicketsList")
        if isinstance(mt, list):
            tmp: List[int] = []
            for it in mt:
                if isinstance(it, dict):
                    for k in ("id", "ticketId", "ticketID", "ticket_id"):
                        if k in it:
                            try:
                                tmp.append(int(it[k]))
                            except Exception:
                                pass
                            break
            merged_ids = tmp

    rows: List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = []
    if merged_ids:
        for mid in merged_ids:
            try:
                rows.append((int(mid), principal_id_int, merged_at, payload))
            except Exception:
                pass
        return rows

    # Caso 2: consultou o "filho" e veio quem é o "pai"
    merged_into = (
        raw.get("mergedIntoId")
        or raw.get("mergedIntoTicketId")
        or raw.get("mainTicketId")
        or raw.get("mainTicketID")
        or raw.get("principalTicketId")
        or raw.get("principalId")
        or raw.get("mergedInto")
    )
    if merged_into is not None:
        try:
            rows.append((int(queried_id), int(merged_into), merged_at, payload))
        except Exception:
            pass

    return rows


def extract_rows_from_any(data: Any, queried_id: int) -> List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]]:
    if data is None:
        return []
    out: List[Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = []
    if isinstance(data, dict):
        out.extend(extract_merge_rows(queried_id, data))
    elif isinstance(data, list):
        for it in data:
            if isinstance(it, dict):
                out.extend(extract_merge_rows(queried_id, it))
    # dedup por ticket_id (filho)
    dedup: Dict[int, Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = {}
    for r in out:
        dedup[int(r[0])] = r
    return list(dedup.values())


# -------------------------
# Main
# -------------------------
def main():
    logging.basicConfig(level=env_str("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sync-tickets-merged")

    token = env_str("MOVIDESK_TOKEN")
    base_url = env_str("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
    neon_dsn = env_str("NEON_DSN")

    # destino
    dst_schema = env_str("DB_SCHEMA", "visualizacao_resolvidos")
    dst_table = env_str("TABLE_NAME", "tickets_mesclados")

    # origem (só pra âncora)
    src_schema = env_str("SOURCE_SCHEMA", "dados_gerais")
    src_table = env_str("SOURCE_TABLE", "tickets_suporte")
    src_date_col = env_str("SOURCE_DATE_COL", "updated_at")

    # execução
    limit = env_int("MAX_IDS", 50)  # mantive o nome MAX_IDS pra não quebrar seu yml antigo
    limit = max(1, min(100, limit))  # API lista no máximo 100 por página (Movidesk)
    rpm = env_float("RPM", 10.0)
    throttle = 60.0 / rpm if rpm > 0 else 0.0
    http_timeout = env_int("HTTP_TIMEOUT", 45)

    merged_query_keys = [k.strip() for k in env_str("MERGED_QUERY_KEYS", "ticketId,id,q").split(",") if k.strip()]

    log.info("script_version=%s limit=%d rpm=%.2f", SCRIPT_VERSION, limit, rpm)

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

        # 2) pega 50 tickets mais recentes da API até a âncora
        candidates, chosen, last_err = list_recent_tickets_from_api_up_to_anchor(
            sess=sess,
            base_url=base_url,
            token=token,
            anchor_ts=anchor_ts,
            limit=limit,
            timeout=http_timeout,
            log=log,
        )

        log.info("api_candidates=%d chosen=%s last_err=%s", len(candidates), chosen, (last_err or ""))

        if not candidates:
            log.info("done_no_candidates")
            return

        # 3) consulta merged para cada candidato e grava
        status_counts: Dict[int, int] = {}
        rows_map: Dict[int, Tuple[int, int, Optional[dt.datetime], psycopg2.extras.Json]] = {}

        checked = 0
        found = 0

        for (ticket_id, upd) in candidates:
            checked += 1

            data, st = movidesk_get_merged(sess, base_url, token, int(ticket_id), http_timeout, merged_query_keys)
            if st is not None:
                status_counts[st] = status_counts.get(st, 0) + 1

            rows = extract_rows_from_any(data, int(ticket_id))
            if rows:
                found += len(rows)
                for r in rows:
                    # dedup por ticket_id filho + pick_row por data
                    old = rows_map.get(int(r[0]))
                    rows_map[int(r[0])] = pick_row(old, r)

            if throttle > 0:
                time.sleep(throttle)

        upserted = upsert_mesclados(conn, dst_schema, dst_table, list(rows_map.values()))
        conn.commit()

        log.info(
            "done checked=%d merges_found=%d upserted=%d statuses=%s",
            checked, found, upserted, json.dumps(status_counts, ensure_ascii=False),
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
