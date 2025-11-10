#!/usr/bin/env python3
import os
import time
import logging
from typing import List, Dict, Iterable

import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"

# Lote “macro” (o código divide sozinho se a API reclamar do limite de nós)
BATCH = int(os.getenv("DETAIL_BATCH", "60"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.20"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)5s  %(message)s"
)

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"missing required env: {name}")
    return v

def fetch_missing_ids(conn) -> List[int]:
    """IDs pendentes para preencher em tickets_resolvidos."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ticket_id
            FROM visualizacao_resolvidos.audit_recent_missing
            WHERE table_name = 'tickets_resolvidos'
            ORDER BY ticket_id
            LIMIT 2000
        """)
        return [r[0] for r in cur.fetchall()]

def chunks(xs: List[int], n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def _request_tickets_by_ids(ids: List[int], token: str) -> List[Dict]:
    """Faz 1 request ao Movidesk para um conjunto de IDs (usando OR)."""
    if not ids:
        return []
    # Monta filtro no formato aceito pelo OData do Movidesk
    filter_expr = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "token": token,
        "$select": "id,status,resolvedIn,closedIn,canceledIn",
        "$filter": filter_expr
    }
    time.sleep(THROTTLE)  # respeita rate-limit
    r = requests.get(f"{API_BASE}/tickets", params=params, timeout=60)
    return r

def movidesk_get_basic(ids: List[int], token: str) -> List[Dict]:
    """
    Busca campos básicos para os tickets informados.
    Se a API devolver o erro do 'node count limit', divide o lote e tenta novamente.
    """
    if not ids:
        return []

    r = _request_tickets_by_ids(ids, token)

    # Tenta dividir automaticamente se estourar o limite de nós do OData
    if r.status_code == 400 and "node count limit" in r.text.lower() and len(ids) > 1:
        mid = len(ids) // 2
        left  = movidesk_get_basic(ids[:mid], token)
        right = movidesk_get_basic(ids[mid:], token)
        return left + right

    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")

    data = r.json()
    out = []
    for t in data:
        out.append({
            "id": t.get("id"),
            "status": t.get("status"),
            "resolvedIn": t.get("resolvedIn"),
            "closedIn": t.get("closedIn"),
            "canceledIn": t.get("canceledIn"),  # mapeado para last_cancelled_at
        })
    return out

def upsert_tickets(conn, rows: List[Dict]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO visualizacao_resolvidos.tickets_resolvidos
        (ticket_id, status, last_resolved_at, last_closed_at, last_cancelled_at)
    VALUES %s
    ON CONFLICT (ticket_id) DO UPDATE SET
        status = EXCLUDED.status,
        last_resolved_at  = COALESCE(EXCLUDED.last_resolved_at,  visualizacao_resolvidos.tickets_resolvidos.last_resolved_at),
        last_closed_at    = COALESCE(EXCLUDED.last_closed_at,    visualizacao_resolvidos.tickets_resolvidos.last_closed_at),
        last_cancelled_at = COALESCE(EXCLUDED.last_cancelled_at, visualizacao_resolvidos.tickets_resolvidos.last_cancelled_at)
    """
    values = [
        (
            r["id"],
            r.get("status"),
            r.get("resolvedIn"),
            r.get("closedIn"),
            r.get("canceledIn"),
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=100)
    return len(values)

def delete_from_missing(conn, ids: List[int]):
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM visualizacao_resolvidos.audit_recent_missing
             WHERE table_name = 'tickets_resolvidos'
               AND ticket_id = ANY(%s)
        """, (ids,))
    logging.info("audit_recent_missing: removidos %d ids (tickets_resolvidos)", len(ids))

def main():
    token = env("MOVIDESK_TOKEN")
    dsn = env("NEON_DSN")

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False

        ids = fetch_missing_ids(conn)
        if not ids:
            logging.info("Sem pendências em audit_recent_missing para tickets_resolvidos.")
            return

        total = 0
        for grp in chunks(ids, BATCH):
            data = movidesk_get_basic(grp, token)
            # Garante que só upsertamos IDs requisitados
            allowed = set(grp)
            data = [d for d in data if d.get("id") in allowed]
            total += upsert_tickets(conn, data)
            delete_from_missing(conn, grp)
            conn.commit()

        logging.info("Upsert concluído: %d registros aplicados em tickets_resolvidos.", total)

if __name__ == "__main__":
    main()
