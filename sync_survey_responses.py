import os
import time
import datetime
import logging

import requests
import psycopg2
from psycopg2.extras import execute_values, Json

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

PAGE_SIZE = max(1, min(100, int(os.getenv("MOVIDESK_PAGE_SIZE", "100"))))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
TICKET_THROTTLE = float(os.getenv("MOVIDESK_TICKET_THROTTLE", "0.15"))
TICKETS_CHUNK = max(1, min(25, int(os.getenv("MOVIDESK_TICKETS_CHUNK", "10"))))

SURVEY_TYPE = int(os.getenv("MOVIDESK_SURVEY_TYPE", "2"))
DAYS_BACK = int(os.getenv("MOVIDESK_SURVEY_DAYS", "120"))
OVERLAP_MIN = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))

SUPPORT_TEAM_MATCH = (os.getenv("SUPPORT_TEAM_MATCH", "suporte") or "suporte").strip().lower()

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN nos secrets.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("tickets_satisfacao")

S = requests.Session()
S.headers.update({"Accept": "application/json"})


def req_json(url, params=None, timeout=90):
    while True:
        r = S.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("Throttle HTTP %s, aguardando %ss…", r.status_code, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json() if r.text else None


def req_list(url, params=None, timeout=90):
    while True:
        r = S.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("Throttle HTTP %s, aguardando %ss…", r.status_code, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []


def iint(x):
    try:
        s = str(x).strip()
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def parse_dt(x):
    if not x:
        return None
    s = str(x).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def to_iso_z(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.replace(microsecond=0).astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def team_name(v):
    if not v:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return v.get("name") or v.get("businessName") or v.get("title") or str(v)
    return str(v)


def is_support_team(name):
    if not name:
        return False
    return SUPPORT_TEAM_MATCH in str(name).strip().lower()


def pick_person(v):
    if not isinstance(v, dict):
        return None
    pid = iint(v.get("id"))
    name = v.get("businessName") or v.get("name") or v.get("email")
    return {"id": pid, "name": name}


def merged_table_info(conn):
    with conn.cursor() as cur:
        cur.execute("select to_regclass('visualizacao_resolvidos.tickets_mesclados')")
        ok = cur.fetchone()[0] is not None
        if not ok:
            return None
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'visualizacao_resolvidos'
              and table_name = 'tickets_mesclados'
            """
        )
        cols = {r[0] for r in cur.fetchall()}
    src = "ticket_id" if "ticket_id" in cols else None
    dst_candidates = ["merged_into_id", "mergedIntoId", "merged_into", "mergedInto", "merged_ticket_id", "mergedTicketId"]
    dst = next((c for c in dst_candidates if c in cols), None)
    if not src or not dst:
        return None
    return {"schema": "visualizacao_resolvidos", "table": "tickets_mesclados", "src": src, "dst": dst}


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_satisfacao")
        cur.execute(
            """
            create table if not exists visualizacao_satisfacao.tickets_satisfacao(
              id                 text,
              ticket_id           bigint,
              type                integer,
              value               integer,
              question_id         text,
              client_id           text,
              response_date       timestamptz,
              support_agent_id    bigint,
              support_agent_name  text,
              support_team        text,
              support_rule        text,
              raw                 jsonb,
              updated_at          timestamptz default now()
            )
            """
        )
        cur.execute(
            """
            alter table visualizacao_satisfacao.tickets_satisfacao
              add column if not exists id text,
              add column if not exists ticket_id bigint,
              add column if not exists type integer,
              add column if not exists value integer,
              add column if not exists question_id text,
              add column if not exists client_id text,
              add column if not exists response_date timestamptz,
              add column if not exists support_agent_id bigint,
              add column if not exists support_agent_name text,
              add column if not exists support_team text,
              add column if not exists support_rule text,
              add column if not exists raw jsonb,
              add column if not exists updated_at timestamptz
            """
        )
        cur.execute("update visualizacao_satisfacao.tickets_satisfacao set updated_at = now() where updated_at is null")
        cur.execute(
            """
            with r as (
              select
                ctid,
                ticket_id,
                row_number() over (
                  partition by ticket_id
                  order by response_date desc nulls last, updated_at desc nulls last
                ) rn
              from visualizacao_satisfacao.tickets_satisfacao
              where ticket_id is not null
            )
            delete from visualizacao_satisfacao.tickets_satisfacao t
            using r
            where t.ctid = r.ctid
              and r.rn > 1
            """
        )
        cur.execute("drop index if exists visualizacao_satisfacao.ux_tickets_satisfacao_ticket_id")
        cur.execute("drop index if exists ux_tickets_satisfacao_ticket_id")
        cur.execute("create unique index ux_tickets_satisfacao_ticket_id on visualizacao_satisfacao.tickets_satisfacao(ticket_id)")
        cur.execute("create index if not exists idx_tickets_satisfacao_response_date on visualizacao_satisfacao.tickets_satisfacao(response_date)")
        cur.execute("create index if not exists idx_tickets_satisfacao_support_agent on visualizacao_satisfacao.tickets_satisfacao(support_agent_id)")
    conn.commit()


def get_since_from_db(conn):
    with conn.cursor() as cur:
        cur.execute("select max(response_date) from visualizacao_satisfacao.tickets_satisfacao")
        row = cur.fetchone()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    floor = now_utc - datetime.timedelta(days=DAYS_BACK)
    if row and row[0]:
        since = row[0] - datetime.timedelta(minutes=OVERLAP_MIN)
        return max(since, floor)
    return floor


def fetch_survey_responses(since_iso):
    url = f"{API_BASE}/survey/responses"
    starting_after = None
    items = []
    while True:
        params = {
            "token": API_TOKEN,
            "type": SURVEY_TYPE,
            "responseDateGreaterThan": since_iso,
            "limit": PAGE_SIZE,
        }
        if starting_after:
            params["startingAfter"] = starting_after
        page = req_json(url, params=params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(THROTTLE)
    return items


def fetch_merge_map(conn, ids, mi):
    if not mi or not ids:
        return {}
    with conn.cursor() as cur:
        q = f"""
        select {mi["src"]}::bigint as src, {mi["dst"]}::bigint as dst
        from {mi["schema"]}.{mi["table"]}
        where {mi["src"]} = any(%s)
          and {mi["dst"]} is not null
        """
        cur.execute(q, (ids,))
        rows = cur.fetchall()
    return {int(a): int(b) for a, b in rows if a is not None and b is not None}


def resolve_canonical_ticket_ids(conn, ticket_ids):
    ids = sorted({int(x) for x in ticket_ids if str(x).isdigit()})
    if not ids:
        return {}
    mi = merged_table_info(conn)
    if not mi:
        return {i: i for i in ids}
    canonical = {i: i for i in ids}
    for _ in range(12):
        current = sorted({canonical[i] for i in canonical})
        m = fetch_merge_map(conn, current, mi)
        if not m:
            break
        changed = False
        for orig in list(canonical.keys()):
            c = canonical[orig]
            if c in m and m[c] != c:
                canonical[orig] = m[c]
                changed = True
        if not changed:
            break
    return canonical


def fetch_tickets_batch(ids):
    url = f"{API_BASE}/tickets"
    filt = " or ".join([f"id eq {int(i)}" for i in ids])
    params = {
        "token": API_TOKEN,
        "$select": "id,ownerTeam,owner",
        "$expand": "owner,ownerHistories",
        "$filter": filt,
        "$top": max(1, len(ids)),
        "$skip": 0,
    }
    return req_list(url, params=params) or []


def safe_fetch_tickets(ids):
    ids = [int(x) for x in ids if str(x).isdigit()]
    out = {}

    def do(batch):
        items = fetch_tickets_batch(batch)
        for t in items:
            tid = iint((t or {}).get("id"))
            if tid is not None:
                out[tid] = t

    def rec(batch):
        try:
            do(batch)
        except requests.HTTPError as e:
            sc = getattr(getattr(e, "response", None), "status_code", None)
            if sc == 400 and len(batch) > 1:
                mid = len(batch) // 2
                rec(batch[:mid])
                rec(batch[mid:])
            elif sc == 400 and len(batch) == 1:
                try:
                    do(batch)
                except Exception:
                    pass
            else:
                raise

    for i in range(0, len(ids), TICKETS_CHUNK):
        rec(ids[i : i + TICKETS_CHUNK])
        time.sleep(TICKET_THROTTLE)

    return out


def owner_histories(ticket):
    v = (ticket or {}).get("ownerHistories")
    if isinstance(v, list):
        return v
    v = (ticket or {}).get("owner_histories")
    if isinstance(v, list):
        return v
    return []


def compute_support_responsible(ticket, response_date_iso):
    resp_dt = parse_dt(response_date_iso)
    histories = []
    for h in owner_histories(ticket):
        if not isinstance(h, dict):
            continue
        cdt = parse_dt(h.get("changedDate"))
        oteam = team_name(h.get("ownerTeam"))
        own = pick_person(h.get("owner"))
        if cdt is not None and own:
            histories.append({"changedDate": cdt, "ownerTeam": oteam, "owner": own})
    histories.sort(key=lambda x: x["changedDate"])

    def owner_at(dt):
        cur = None
        for h in histories:
            if h["changedDate"] <= dt:
                cur = h
        return cur

    def last_support_before(dt):
        best = None
        for h in histories:
            if h["changedDate"] <= dt and is_support_team(h.get("ownerTeam")):
                best = h
        return best

    if resp_dt:
        at = owner_at(resp_dt)
        if at and is_support_team(at.get("ownerTeam")):
            o = at["owner"]
            return {"agentId": o.get("id"), "agentName": o.get("name"), "team": at.get("ownerTeam"), "rule": "at_response_support"}

        last_sup = last_support_before(resp_dt)
        if last_sup:
            o = last_sup["owner"]
            return {"agentId": o.get("id"), "agentName": o.get("name"), "team": last_sup.get("ownerTeam"), "rule": "last_support_before_response"}

    for h in reversed(histories):
        if is_support_team(h.get("ownerTeam")):
            o = h["owner"]
            return {"agentId": o.get("id"), "agentName": o.get("name"), "team": h.get("ownerTeam"), "rule": "last_support_any"}

    return {"agentId": None, "agentName": None, "team": None, "rule": "no_support_found"}


def enrich_items(items, ticket_map, canonical_map):
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        orig_tid = iint(it.get("ticketId"))
        canon_tid = canonical_map.get(orig_tid, orig_tid) if orig_tid is not None else None

        t = None
        if orig_tid is not None:
            t = ticket_map.get(orig_tid)
        if t is None and canon_tid is not None:
            t = ticket_map.get(canon_tid)

        support = compute_support_responsible(t or {}, it.get("responseDate")) if orig_tid is not None else None

        enriched = dict(it)
        enriched["originalTicketId"] = orig_tid
        enriched["canonicalTicketId"] = canon_tid
        if support:
            enriched["supportResponsible"] = support
        out.append(enriched)
    return out


def upsert_rows(conn, items):
    if not items:
        return 0

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    values = []
    for it in items:
        canon_tid = iint(it.get("canonicalTicketId"))
        if canon_tid is None:
            continue

        rid = str(it.get("id") or "").strip() or None
        rtype = iint(it.get("type"))
        val = iint(it.get("value"))
        qid = str(it.get("questionId") or "").strip() or None
        cid = str(it.get("clientId") or "").strip() or None
        rdate = it.get("responseDate")

        support = it.get("supportResponsible") if isinstance(it.get("supportResponsible"), dict) else None
        s_agent_id = iint(support.get("agentId")) if support else None
        s_agent_name = support.get("agentName") if support else None
        s_team = support.get("team") if support else None
        s_rule = support.get("rule") if support else None

        values.append(
            (
                canon_tid,
                rid,
                rtype,
                val,
                qid,
                cid,
                rdate,
                s_agent_id,
                s_agent_name,
                s_team,
                s_rule,
                Json(it),
                now_iso,
            )
        )

    if not values:
        return 0

    sql = """
    insert into visualizacao_satisfacao.tickets_satisfacao
      (ticket_id, id, type, value, question_id, client_id, response_date,
       support_agent_id, support_agent_name, support_team, support_rule, raw, updated_at)
    values %s
    on conflict (ticket_id) do update set
      id = excluded.id,
      type = excluded.type,
      value = excluded.value,
      question_id = excluded.question_id,
      client_id = excluded.client_id,
      response_date = excluded.response_date,
      support_agent_id = excluded.support_agent_id,
      support_agent_name = excluded.support_agent_name,
      support_team = excluded.support_team,
      support_rule = excluded.support_rule,
      raw = excluded.raw,
      updated_at = excluded.updated_at
    where visualizacao_satisfacao.tickets_satisfacao.response_date is null
       or excluded.response_date >= visualizacao_satisfacao.tickets_satisfacao.response_date
    """

    with conn.cursor() as cur:
        cur.execute("set local synchronous_commit=off")
        execute_values(cur, sql, values, page_size=200)

    conn.commit()
    return len(values)


def main():
    logger.info("Iniciando sync de tickets_satisfacao (1 nota por ticket + merge + responsavel suporte).")

    with psycopg2.connect(NEON_DSN) as conn:
        ensure_table(conn)

        since_dt = get_since_from_db(conn)
        since_iso = to_iso_z(since_dt)

        items = fetch_survey_responses(since_iso)
        logger.info("Survey retornou %s respostas desde %s", len(items), since_iso)

        orig_ids = sorted({iint(x.get("ticketId")) for x in items if isinstance(x, dict) and iint(x.get("ticketId")) is not None})
        canonical_map = resolve_canonical_ticket_ids(conn, orig_ids)

        canon_ids = sorted({canonical_map.get(i, i) for i in orig_ids if i is not None})
        fetch_ids = sorted(set(orig_ids).union(set(canon_ids)))

        logger.info("Tickets únicos para enriquecer: %s", len(fetch_ids))
        tickets_map = safe_fetch_tickets(fetch_ids) if fetch_ids else {}

        enriched = enrich_items(items, tickets_map, canonical_map)
        n = upsert_rows(conn, enriched)

    logger.info("Finalizado. DESDE=%s | upsert=%s", since_iso, n)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Falha no sync_survey_responses")
        raise
