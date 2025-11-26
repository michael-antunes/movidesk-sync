import os
import time
import requests
import psycopg2
import psycopg2.extras
import datetime

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")


def to_iso_z(dt):
    return dt.isoformat().replace("+00:00", "Z")


def get_since_from_db(conn, days_back, overlap_minutes):
    with conn.cursor() as cur:
        cur.execute(
            """
            select max(changed_date)
            from visualizacao_resolucao.resolucao_por_status
        """
        )
        max_date = cur.fetchone()[0]
    if max_date:
        since_dt = max_date - datetime.timedelta(minutes=overlap_minutes)
    else:
        since_dt = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(days=days_back)
    return since_dt


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def _req(url, params, timeout=90):
    while True:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:1200])
            except Exception:
                pass
            r.raise_for_status()
        return r.json() if r.text else []


def fetch_status_history(since_iso):
    url = f"{API_BASE}/tickets/statusHistory"
    limit = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
    throttle = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
    starting_after = None
    items = []
    while True:
        params = {
            "token": API_TOKEN,
            "changedDateGreaterThan": since_iso,
            "limit": limit,
        }
        if starting_after:
            params["startingAfter"] = starting_after
        page = _req(url, params) or {}
        page_items = page.get("items") or []
        items.extend(page_items)
        if not page_items or not bool(page.get("hasMore")):
            break
        starting_after = page_items[-1].get("id")
        time.sleep(throttle)
    return items


def map_row(r):
    return {
        "id": str(r.get("id") or ""),
        "ticket_id": iint(r.get("ticketId")),
        "agent_name": r.get("agentName"),
        "changed_date": r.get("changedDate"),
        "status": r.get("status"),
        "permanency_time_fulltime_seconds": iint(
            r.get("permanencyTimeFulltimeSeconds")
        ),
    }


UPSERT_SQL = """
insert into visualizacao_resolucao.resolucao_por_status
(id,ticket_id,agent_name,changed_date,status,permanency_time_fulltime_seconds)
values (%(id)s,%(ticket_id)s,%(agent_name)s,%(changed_date)s,%(status)s,%(permanency_time_fulltime_seconds)s)
on conflict (id) do update set
  ticket_id = excluded.ticket_id,
  agent_name = excluded.agent_name,
  changed_date = excluded.changed_date,
  status = excluded.status,
  permanency_time_fulltime_seconds = excluded.permanency_time_fulltime_seconds
"""


def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")
    days_back = int(os.getenv("MOVIDESK_RPS_DAYS", "120"))
    overlap_minutes = int(os.getenv("MOVIDESK_OVERLAP_MIN", "10080"))

    conn = psycopg2.connect(NEON_DSN)
    try:
        since_dt = get_since_from_db(conn, days_back, overlap_minutes)
        since_iso = to_iso_z(since_dt)

        resp = fetch_status_history(since_iso)
        rows = [map_row(x) for x in resp if isinstance(x, dict)]
        n = upsert_rows(conn, rows)

        print(f"DESDE {since_iso} | UPSERT: {n}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
