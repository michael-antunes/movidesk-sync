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
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
BULK_TOP = int(os.getenv("BULK_TOP", "1000"))
MAX_LOOPS = int(os.getenv("MAX_LOOPS", "25"))
SLEEP_BETWEEN_LOOPS = float(os.getenv("SLEEP_BETWEEN_LOOPS", "0.2"))

RANGE_SCAN_LOCK_NAME = os.getenv("RANGE_SCAN_LOCK_NAME", f"{SCHEMA}.range_scan_control")
USE_DB_ADVISORY_LOCK = os.getenv("USE_DB_ADVISORY_LOCK", "true").lower() in ("1", "true", "yes", "y")


def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
               ultima_data_validada,
               COALESCE(ultima_data_validada, data_inicio) AS ultima,
               id_inicial,
               id_final,
               id_atual
          FROM {SCHEMA}.range_scan_control
         LIMIT 1;
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    data_inicio, data_fim, ultima_raw, ultima, id_inicial, id_final, id_atual = row
    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "ultima_raw": ultima_raw,
        "ultima": ultima,
        "id_inicial": id_inicial,
        "id_final": id_final,
        "id_atual": id_atual,
    }


def set_ultima_validada(cur, nv, expected) -> int:
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


def http_get(url, params):
    for attempt in (1, 2, 3):
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        time.sleep(0.8 * attempt)
    r.raise_for_status()


def fetch_hits(data_fim, ultima):
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
    return http_get(url, params) or []


def do_one_cycle():
    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            ctrl = read_control(cur)
            if not ctrl:
                return 0

            data_inicio = ctrl["data_inicio"]
            data_fim = ctrl["data_fim"]
            ultima = ctrl["ultima"]

            if ultima <= data_fim:
                return 0

            hits = fetch_hits(data_fim, ultima)

            if not hits:
                nv = data_fim
            else:
                min_lu = min(datetime.fromisoformat(x["lastUpdate"].replace("Z", "+00:00")) for x in hits)
                nv = min_lu - timedelta(seconds=1)
                if nv < data_fim:
                    nv = data_fim

            rc = set_ultima_validada(cur, nv, ctrl)
            if rc == 0:
                return 0

            return len(hits)


def main():
    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if USE_DB_ADVISORY_LOCK and not try_advisory_lock(cur):
                return

    loops = 0
    while loops < MAX_LOOPS:
        hits = do_one_cycle()
        loops += 1
        if hits == 0:
            break
        time.sleep(SLEEP_BETWEEN_LOOPS)

    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if USE_DB_ADVISORY_LOCK:
                advisory_unlock(cur)


if __name__ == "__main__":
    main()
