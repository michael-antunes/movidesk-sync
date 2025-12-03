import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

PAGE_SIZE = int(os.getenv("ABERTOS_PAGE_SIZE", "100"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("abertos_index")


def md_get(path_or_full, params=None):
    """Chamada básica para a API Movidesk, com retry para 429/5xx."""
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full.lstrip('/')}"
    p = dict(params or {})
    p["token"] = TOKEN

    resp = requests.get(url, params=p, timeout=60)
    if resp.status_code in (429, 500, 502, 503, 504):
        logger.warning("Retry em %s por status %s", url, resp.status_code)
        time.sleep(1.5)
        resp = requests.get(url, params=p, timeout=60)

    resp.raise_for_status()
    data = resp.json()
    return data or []


def ensure_table(conn):
    """Garante estrutura de visualizacao_atual.tickets_abertos e remove colunas antigas de erro."""
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_atual")

        cur.execute(
            """
            create table if not exists visualizacao_atual.tickets_abertos(
              ticket_id       bigint primary key,
              last_update     timestamptz,
              base_status     text,
              updated_at      timestamptz default now(),
              raw             jsonb,
              raw_last_update timestamptz
            )
            """
        )

        # Garantir colunas (idempotente)
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "add column if not exists last_update timestamptz"
        )
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "add column if not exists base_status text"
        )
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "add column if not exists updated_at timestamptz default now()"
        )
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "add column if not exists raw jsonb"
        )
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "add column if not exists raw_last_update timestamptz"
        )

        # Remover colunas de erro que você não quer mais
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "drop column if exists raw_error_at"
        )
        cur.execute(
            "alter table visualizacao_atual.tickets_abertos "
            "drop column if exists raw_error_msg"
        )

    conn.commit()


def upsert_page(conn, rows):
    if not rows:
        return 0

    sql = """
        insert into visualizacao_atual.tickets_abertos
          (ticket_id, last_update, base_status, updated_at)
        values %s
        on conflict (ticket_id) do update set
          last_update = excluded.last_update,
          base_status = excluded.base_status,
          updated_at  = excluded.updated_at
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=len(rows))
    conn.commit()
    return len(rows)


def cleanup_not_open(conn, open_ids):
    """
    Remove da tabela qualquer ticket que NÃO esteja na lista de abertos
    retornada agora pela API.
    """
    if not open_ids:
        # Nenhum ticket aberto -> limpa tudo
        with conn.cursor() as cur:
            cur.execute("delete from visualizacao_atual.tickets_abertos")
            removed = cur.rowcount
        conn.commit()
        if removed:
            logger.info("Removendo %s tickets (nenhum aberto na API).", removed)
        return removed

    with conn.cursor() as cur:
        cur.execute("create temporary table tmp_open_ids(ticket_id bigint primary key) on commit drop")

        # Inserir todos os IDs abertos da rodada atual
        unique_ids = list({i for i in open_ids if i is not None})
        execute_values(
            cur,
            "insert into tmp_open_ids(ticket_id) values %s",
            [(i,) for i in unique_ids],
            page_size=1000,
        )

        # Delete de qualquer ticket que não apareceu agora como aberto
        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             where not exists (
               select 1 from tmp_open_ids t
                where t.ticket_id = ta.ticket_id
             )
            """
        )
        removed = cur.rowcount

    conn.commit()
    if removed:
        logger.info("Removendo %s tickets que não estão mais abertos.", removed)
    return removed


def cleanup_merged(conn):
    """
    Remove da tabela de abertos qualquer ticket que esteja marcado como mesclado.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from visualizacao_atual.tickets_abertos ta
             using visualizacao_resolvidos.tickets_mesclados tm
             where ta.ticket_id = tm.ticket_id
            """
        )
        removed = cur.rowcount
    conn.commit()
    if removed:
        logger.info("Removendo %s tickets que estão marcados como mesclados.", removed)
    return removed


def sync_index(conn):
    logger.info("Iniciando sync de índice de tickets abertos (page_size=%s).", PAGE_SIZE)

    open_ids = []
    total_upserted = 0
    skip = 0

    while True:
        logger.info("Listando tickets abertos em /tickets com skip=%s, top=%s", skip, PAGE_SIZE)
        params = {
            # Apenas tickets cujo baseStatus NÃO é resolvido/fechado/cancelado
            "$filter": "(baseStatus ne 'Resolved' and baseStatus ne 'Closed' and baseStatus ne 'Canceled')",
            "$orderby": "id",
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$select": "id,lastUpdate,baseStatus",
            # NÃO passamos includeDeletedItems=true aqui justamente pra não trazer apagados
        }

        data = md_get("tickets", params)
        if not data:
            break

        rows = []
        now_utc = datetime.now(timezone.utc)
        for item in data:
            tid = item.get("id")
            if tid is None:
                continue
            try:
                tid_int = int(tid)
            except Exception:
                continue

            last_update = item.get("lastUpdate")
            base_status = item.get("baseStatus")

            rows.append((tid_int, last_update, base_status, now_utc))
            open_ids.append(tid_int)

        upserted = upsert_page(conn, rows)
        total_upserted += upserted

        if len(data) < PAGE_SIZE:
            break

        skip += PAGE_SIZE
        time.sleep(THROTTLE)

    logger.info("Sync de índice: %s registros upsertados. Limpando fechados/apagados/mesclados…", total_upserted)
    removed_closed = cleanup_not_open(conn, open_ids)
    removed_merged = cleanup_merged(conn)

    logger.info(
        "Sync índice concluído. Sucesso=%s, Removidos(fecharam/apagados)=%s, Removidos(mesclados)=%s.",
        total_upserted,
        removed_closed,
        removed_merged,
    )

    return open_ids


def main():
    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        sync_index(conn)


if __name__ == "__main__":
    main()
