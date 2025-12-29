import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from contextlib import closing


SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
NEON_DSN = os.environ["NEON_DSN"]
MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "1.2"))

BULK_TOP = int(os.getenv("BULK_TOP", "1000"))
MAX_LOOPS = int(os.getenv("MAX_LOOPS", "25"))
SLEEP_BETWEEN_LOOPS = float(os.getenv("SLEEP_BETWEEN_LOOPS", "0.2"))

FAIL_OPEN = os.getenv("FAIL_OPEN", "true").lower() in ("1", "true", "yes", "y")

RANGE_SCAN_LOCK_NAME = os.getenv("RANGE_SCAN_LOCK_NAME", f"{SCHEMA}.range_scan_control")
USE_DB_ADVISORY_LOCK = os.getenv("USE_DB_ADVISORY_LOCK", "true").lower() in ("1", "true", "yes", "y")


def as_aware_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    dt = as_aware_utc(dt)
    return dt.isoformat().replace("+00:00", "Z")


def try_advisory_lock(cur) -> bool:
    cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (RANGE_SCAN_LOCK_NAME,))
    return bool(cur.fetchone()[0])


def advisory_unlock(cur) -> None:
    cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (RANGE_SCAN_LOCK_NAME,))


def read_control(cur):
    cur.execute(
        f"""
        SELECT data_inicio,
               data_fim,
               ultima_data_validada
          FROM {SCHEMA}.range_scan_control
         LIMIT 1;
        """
    )
    row = cur.fetchone()
    if not row:
        return None

    data_inicio = as_aware_utc(row[0])
    data_fim = as_aware_utc(row[1])
    ultima_raw = as_aware_utc(row[2])
    ultima = ultima_raw or data_inicio

    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "ultima_raw": ultima_raw,
        "ultima": ultima,
    }


def set_ultima_validada(cur, nv: datetime, expected) -> int:
    cur.execute(
        f"""
        UPDATE {SCHEMA}.range_scan_control
           SET ultima_data_validada = %s
         WHERE data_inicio = %s
           AND data_fim = %s
           AND ultima_data_validada IS NOT DISTINCT FROM %s;
        """,
        (nv, expected["data_inicio"], expected["data_fim"], expected["ultima_raw"]),
    )
    return cur.rowcount


def parse_last_update(s: str) -> datetime:
    s = str(s).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return as_aware_utc(dt)


def http_get_json(session: requests.Session, url: str, params: dict):
    last_err = None
    for i in range(HTTP_RETRIES):
        try:
            r = session.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(HTTP_BACKOFF * (i + 1))
                continue
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(HTTP_BACKOFF * (i + 1))
    if FAIL_OPEN:
        return None
    raise last_err


def fetch_hits(session: requests.Session, data_fim: datetime, ultima: datetime):
    flt = (
        f"lastUpdate ge {iso_z(data_fim)} and lastUpdate le {iso_z(ultima)} and "
        "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')"
    )
    url = f"{API_BASE}/tickets"
    params = {
        "token": MOVIDESK_TOKEN,
        "$filter": flt,
        "$select": "id,lastUpdate,baseStatus",
        "$orderby": "lastUpdate desc",
        "$top": str(BULK_TOP),
    }
    data = http_get_json(session, url, params)
    if data is None:
        return None
    return data or []


def do_one_cycle(session: requests.Session) -> int:
    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            ctrl = read_control(cur)
            if not ctrl:
                return 0

            data_fim = ctrl["data_fim"]
            ultima = ctrl["ultima"]

            if ultima <= data_fim:
                return 0

            hits = fetch_hits(session, data_fim, ultima)
            if hits is None:
                return 0

            if not hits:
                nv = data_fim
            else:
                min_lu = min(parse_last_update(x["lastUpdate"]) for x in hits if x.get("lastUpdate"))
                nv = min_lu - timedelta(seconds=1)
                if nv < data_fim:
                    nv = data_fim

            rc = set_ultima_validada(cur, nv, ctrl)
            if rc == 0:
                return 0

            return len(hits)


def main():
    locked = False
    try:
        if USE_DB_ADVISORY_LOCK:
            with closing(psycopg2.connect(NEON_DSN)) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    if not try_advisory_lock(cur):
                        return
                    locked = True

        session = requests.Session()
        session.headers.update({"Accept": "application/json"})

        loops = 0
        while loops < MAX_LOOPS:
            try:
                hits = do_one_cycle(session)
            except requests.exceptions.RequestException:
                if FAIL_OPEN:
                    break
                raise
            loops += 1
            if hits == 0:
                break
            time.sleep(SLEEP_BETWEEN_LOOPS)

    finally:
        if USE_DB_ADVISORY_LOCK and locked:
            with closing(psycopg2.connect(NEON_DSN)) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    advisory_unlock(cur)


if __name__ == "__main__":
    main()
