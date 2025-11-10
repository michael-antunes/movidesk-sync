#!/usr/bin/env python3
import os
import sys
import time
import math
import json
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

import requests
import psycopg2
from psycopg2.extras import execute_values

LOG = logging.getLogger("sync_resolved_detail")
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)5s  %(message)s",
)

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ["MOVIDESK_TOKEN"]
NEON_DSN = os.environ["NEON_DSN"]
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))  # segundos

# ---- Helpers ---------------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc)

def odata_ts(dt: datetime) -> str:
    """
    Movidesk aceita DateTimeOffset sem aspas; use 'Z' para UTC.
    Ex: 2016-09-01T00:00:00.00Z  (vide docs de exemplos de OData).
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.00Z")

def od_get(path: str, params: dict):
    # usa $top/$skip; NÃO use $take/$count (não suportados)
    q = {"token": TOKEN}
    q.update(params or {})
    url = f"{BASE}/{path}?{urlencode(q, safe=\"(),$:+-:TZ \")}"
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    return r.json()

def page_iter(odata_filter: str, select_fields: str, page=1000):
    skip = 0
    while True:
        params = {
            "$select": select_fields,
            "$filter": odata_filter,
            "$orderby": "id desc",
            "$top": page,
            "$skip": skip,
        }
        data = od_get("tickets", params)
        if not data:
            break
        yield data
        skip += page
        time.sleep(THROTTLE)

# ---- DB --------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO visualizacao_resolvidos.tickets_resolvidos
  (ticket_id, status, last_resolved_at, last_closed_at, last_cancelled_at)
VALUES %s
ON CONFLICT (ticket_id) DO UPDATE SET
  status = EXCLUDED.status,
  last_resolved_at = COALESCE(EXCLUDED.last_resolved_at,
                              visualizacao_resolvidos.tickets_resolvidos.last_resolved_at),
  last_closed_at   = COALESCE(EXCLUDED.last_closed_at,
                              visualizacao_resolvidos.tickets_resolvidos.last_closed_at),
  last_cancelled_at= COALESCE(EXCLUDED.last_cancelled_at,
                              visualizacao_resolvidos.tickets_resolvidos.last_cancelled_at)
"""

SYNC_READ_SQL = """
SELECT last_update
  FROM visualizacao_resolvidos.sync_control
 WHERE name = 'tickets_resolvidos'
"""

SYNC_WRITE_SQL = """
INSERT INTO visualizacao_resolvidos.sync_control (name, last_update)
VALUES ('tickets_resolvidos', %s)
ON CONFLICT (name) DO UPDATE SET last_update = EXCLUDED.last_update
"""

# ---- Main ------------------------------------------------------------------

def main():
    # 1) Janela de coleta (a partir do controle; senão, 60 dias)
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        cur.execute(SYNC_READ_SQL)
        row = cur.fetchone()
    if row and row[0]:
        window_from = row[0].astimezone(timezone.utc)
    else:
        window_from = utc_now() - timedelta(days=60)

    window_to = utc_now()
    LOG.info("Janela: %s -> %s", window_from.isoformat(), window_to.isoformat())

    # 2) OData filter: resolvedIn / closedIn / canceledIn na janela
    #    IMPORTANTÍSSIMO: datetimes SEM aspas para não virar string em OData!
    f_from = odata_ts(window_from)
    f_to   = odata_ts(window_to)

    odata_filter = (
        f"(resolvedIn ge {f_from} and resolvedIn le {f_to})"
        f" or (closedIn ge {f_from} and closedIn le {f_to})"
        f" or (canceledIn ge {f_from} and canceledIn le {f_to})"
    )

    # 3) Campos que realmente precisamos (inclui canceledIn!)
    select_fields = ",".join([
        "id","status","resolvedIn","closedIn","canceledIn"
    ])

    # 4) Coleta
    rows = []
    for page in page_iter(odata_filter, select_fields, page=1000):
        for t in page:
            tid = t.get("id")
            if not isinstance(tid, int):
                continue
            status = t.get("status")
            r_in   = t.get("resolvedIn")
            c_in   = t.get("closedIn")
            x_in   = t.get("canceledIn")  # <- **a data de cancelamento**

            # Converte para timezone-aware UTC (psycopg2 aceita string ISO)
            def to_dt(x):
                if not x:
                    return None
                # Movidesk devolve DateTimeOffset; manter como texto ISO
                return x

            rows.append((
                tid,
                status,
                to_dt(r_in),
                to_dt(c_in),
                to_dt(x_in),
            ))

    if not rows:
        LOG.info("Sem alterações para aplicar.")
        # Mesmo sem linhas, atualiza o controle para não repetir a janela toda
        with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
            cur.execute(SYNC_WRITE_SQL, (window_to,))
            conn.commit()
        return

    # 5) Upsert em lote
    with psycopg2.connect(NEON_DSN) as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
        cur.execute(SYNC_WRITE_SQL, (window_to,))
        conn.commit()

    LOG.info("Upsert concluído. Total de tickets: %d", len(rows))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.exception("Falhou")
        sys.exit(1)
