# range_scan_to_audit.py  (FIXED)

import os
import json
import time
import random
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
from contextlib import closing

SCHEMA = os.getenv("SCHEMA", "visualizacao_resolvidos")
NEON_DSN = os.environ["NEON_DSN"]
MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]

API_BASE = "https://api.movidesk.com/public/v1"

BULK_TOP = int(os.getenv("BULK_TOP", "200"))
WINDOW = int(os.getenv("WINDOW", "50"))
MAX_LOOPS = int(os.getenv("MAX_LOOPS", "1"))

HTTP_TIMEOUT = (10, 120)
MAX_ATTEMPTS = 6

RETRYABLE_PGCODES = {"40001", "40P01", "55P03", "57014"}

# ---- LOCK GLOBAL PARA EVITAR CONCORRÊNCIA ENTRE WORKFLOWS ----
RANGE_SCAN_LOCK_NAME = os.getenv("RANGE_SCAN_LOCK_NAME", f"{SCHEMA}.range_scan_control")
USE_DB_ADVISORY_LOCK = os.getenv("USE_DB_ADVISORY_LOCK", "true").lower() in ("1", "true", "yes", "y")

def try_advisory_lock(cur) -> bool:
    cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (RANGE_SCAN_LOCK_NAME,))
    return bool(cur.fetchone()[0])

def advisory_unlock(cur) -> None:
    cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (RANGE_SCAN_LOCK_NAME,))


def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def read_control(cur):
    cur.execute(
        f"""
        SELECT data_inicio,
               data_fim,
               ultima_data_validada,
               COALESCE(ultima_data_validada, data_inicio) AS ultima,
               id_inicial,
               id_final,
               id_atual,
               concluido_em
          FROM {SCHEMA}.range_scan_control
         LIMIT 1;
        """
    )
    row = cur.fetchone()
    if not row:
        return None

    data_inicio, data_fim, ultima_raw, ultima, id_inicial, id_final, id_atual, concluido_em = row
    return {
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "ultima_raw": ultima_raw,
        "ultima": ultima,
        "id_inicial": id_inicial,
        "id_final": id_final,
        "id_atual": id_atual,
        "concluido_em": concluido_em,
    }


def set_ultima_validada(cur, nv, expected) -> int:
    """
    Atualiza ultima_data_validada com controle otimista (evita corrida).
    Retorna rowcount (0 => alguém mexeu no controle entre o read e o update).
    """
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


def fetch_ids_between(data_fim, ultima):
    flt = (
        f"lastUpdate ge {iso_z(data_fim)} and lastUpdate le {iso_z(ultima)} and "
        "(baseStatus eq 'Resolved' or baseStatus eq 'Closed' or baseStatus eq 'Canceled')"
    )
    url = f"{API_BASE}/tickets"
    params = {
        "$filter": flt,
        "$select": "id,lastUpdate,baseStatus",
        "$orderby": "lastUpdate desc",
        "$top": BULK_TOP,
        "token": MOVIDESK_TOKEN,
        "includeDeletedItems": "true",
    }

    for attempt in range(1, MAX_ATTEMPTS + 1):
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        time.sleep(0.8 * attempt)
    r.raise_for_status()


def do_one_cycle():
    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:

            ctrl = read_control(cur)
            if not ctrl:
                return 0, 0, 0

            data_inicio = ctrl["data_inicio"]
            data_fim = ctrl["data_fim"]
            ultima = ctrl["ultima"]

            hits = fetch_ids_between(data_fim, ultima)

            if not hits:
                nv = data_fim
            else:
                min_lu = min(datetime.fromisoformat(x["lastUpdate"].replace("Z", "+00:00")) for x in hits)
                nv = min_lu - timedelta(microseconds=1)

            # clamp defensivo
            lo = min(data_inicio, data_fim)
            hi = max(data_inicio, data_fim)
            if nv < lo:
                nv = lo
            if nv > hi:
                nv = hi

            rc = set_ultima_validada(cur, nv, ctrl)
            if rc == 0:
                print("[range-scan] control mudou durante o ciclo; não atualizei ultima_data_validada.")
                return 0, 0, 0

            return len(hits), len(hits), 0


def main():
    with closing(psycopg2.connect(NEON_DSN)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:

            if USE_DB_ADVISORY_LOCK and not try_advisory_lock(cur):
                print(f"[range-scan] outro workflow já está com lock '{RANGE_SCAN_LOCK_NAME}'. Saindo OK.")
                return

    loops = 0
    while loops < MAX_LOOPS:
        hit, got, ins = do_one_cycle()
        print(f"[range-scan] ciclo={loops+1} hits={hit}")
        loops += 1


if __name__ == "__main__":
    main()
