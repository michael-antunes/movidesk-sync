#!/usr/bin/env python3
import os
import time
import math
import json
import logging
from typing import Iterable, List, Dict

import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
BATCH = 100  # tamanho do lote para o OData "id in (...)"

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
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ticket_id
            FROM visualizacao_resolvidos.audit_recent_missing
            WHERE table_name = 'tickets_resolvidos'
            ORDER BY ticket_id
            LIMIT 1000
        """)
        rows = cur.fetchall()
    return [r[0] for r in rows]

def chunks(ids: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(ids), size):
        yield ids[i:i+size]

def movidesk_get_basic(ids: List[int], token: str) -> List[Dict]:
    """
    Busca campos básicos: id, status, resolvedIn, closedIn, canceledIn
    Observação: em Movidesk o campo é 'canceledIn' (uma letra 'l'),
    e o status retornado para cancelados é 'Canceled'.
    """
    if not ids:
        return []

    q = f"id in ({','.join(str(i) for i in ids)})"
    params = {
        "token": token,
        "$select": "id,status,resolvedIn,closedIn,canceledIn",
        "$filter": q
    }
    r = requests.get(f"{API_BASE}/tickets", params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    data = r.json()
    # Normaliza as chaves e garante ausência vira None
    out = []
    for t in data:
        out.append({
            "id": t.get("id"),
            "status": t.get("status"),
            "resolvedIn": t.get("resolvedIn"),
            "closedIn": t.get("closedIn"),
            "canceledIn": t.get("canceledIn"),
        })
    return out

def upsert_tickets(conn, rows: List[Dict]) -> int:
    if not rows:
        return 0
    # upsert minimalista — só status e 3 timestamps
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
    logging.info("audit_recent_missing: limpos %d ids para table_name=tickets_resolvidos", len(ids))

def main():
    token = env("MOVIDESK_TOKEN")
    dsn   = env("NEON_DSN")

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False

        ids = fetch_missing_ids(conn)
        if not ids:
            logging.info("Nenhum ticket pendente em audit_recent_missing (tickets_resolvidos). Nada a fazer.")
            return

        total_upsert = 0
        for group in chunks(ids, BATCH):
            data = movidesk_get_basic(group, token)
            # segurança: garante que só upsertamos IDs que pedimos
            data = [d for d in data if d.get("id") in group]
            total_upsert += upsert_tickets(conn, data)
            delete_from_missing(conn, group)
            conn.commit()
            time.sleep(float(os.getenv("MOVIDESK_THROTTLE", "0.20")))  # gentil com a API

        logging.info("Upserts em tickets_resolvidos concluídos. Registros aplicados: %d", total_upsert)

if __name__ == "__main__":
    main()
