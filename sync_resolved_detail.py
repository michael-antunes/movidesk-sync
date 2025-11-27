import os
import logging
import time
from typing import List, Dict, Tuple, Set

import requests
import psycopg2
from psycopg2.extras import execute_batch

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("detail")

MOVIDESK_TOKEN = os.environ["MOVIDESK_TOKEN"]
MOVIDESK_BASE_URL = os.environ.get(
    "MOVIDESK_BASE_URL",
    "https://api.movidesk.com/public/v1",
)
PG_DSN = os.environ["PG_DSN"]

BATCH_LIMIT = int(os.environ.get("DETAIL_BATCH_LIMIT", "200"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "4"))


# ----------------------------------------------------------------------
# Chamadas à API do Movidesk
# ----------------------------------------------------------------------
def movidesk_get_ticket(ticket_id: int) -> Dict | None:
    """Busca um ticket no Movidesk. Retorna dict ou None (404/erro)."""
    url = f"{MOVIDESK_BASE_URL}/tickets/{ticket_id}"
    headers = {"Authorization": f"Bearer {MOVIDESK_TOKEN}"}
    params = {
        # campos necessários para preencher tickets_resolvidos
        "$select": "id,status,lastUpdate,resolvedIn,closedIn,canceledIn",
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.warning(
                "detail: erro de rede ao buscar ticket %s: %s",
                ticket_id,
                exc,
            )
            if attempt >= MAX_RETRIES:
                return None
            time.sleep(2**attempt)
            continue

        if resp.status_code == 404:
            # ticket não existe / não acessível
            return None

        if resp.status_code == 429 and attempt < MAX_RETRIES:
            retry_after = int(resp.headers.get("Retry-After", "2"))
            logger.warning(
                "detail: 429 para ticket %s, aguardando %ss",
                ticket_id,
                retry_after,
            )
            time.sleep(retry_after)
            continue

        if not resp.ok:
            logger.warning(
                "detail: resposta inesperada da API (%s) para ticket %s: %s",
                resp.status_code,
                ticket_id,
                resp.text[:200],
            )
            if attempt >= MAX_RETRIES:
                return None
            time.sleep(2**attempt)
            continue

        try:
            return resp.json()
        except ValueError:
            logger.warning(
                "detail: JSON inválido para ticket %s: %r",
                ticket_id,
                resp.text[:200],
            )
            return None


# ----------------------------------------------------------------------
# Buscas no Postgres
# ----------------------------------------------------------------------
def buscar_pendentes_audit(conn, limit: int) -> List[int]:
    """
    Busca pendências em visualizacao_resolvidos.audit_recent_missing
    para a tabela 'tickets_resolvidos'.
    """
    sql = """
        SELECT ticket_id
          FROM visualizacao_resolvidos.audit_recent_missing
         WHERE table_name = 'tickets_resolvidos'
         ORDER BY run_id DESC, ticket_id DESC
         LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def buscar_pendentes_fallback(conn, limit: int) -> List[int]:
    """
    Fallback: busca em visualizacao_resolvidos.tickets_resolvidos
    tickets cujo status exige um timestamp que ainda está NULL.

      - Resolvido  e last_resolved_at IS NULL
      - Fechado    e last_closed_at   IS NULL
      - Cancelado  e last_cancelled_at IS NULL
    """
    sql = """
        SELECT ticket_id
          FROM visualizacao_resolvidos.tickets_resolvidos
         WHERE (status = 'Resolvido' AND last_resolved_at IS NULL)
            OR (status = 'Fechado'  AND last_closed_at   IS NULL)
            OR (status = 'Cancelado' AND last_cancelled_at IS NULL)
         ORDER BY last_update DESC NULLS LAST
         LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def obter_lote_pendentes(conn, limit: int) -> Tuple[List[int], Set[int]]:
    """
    Monta um lote de até `limit` tickets:

      1. tenta audit_recent_missing
      2. se sobrar espaço, complementa com fallback em tickets_resolvidos

    Retorna:
      - lista de ticket_ids (ordem: audit primeiro)
      - conjunto dos IDs que vieram do audit (para limpar depois)
    """
    from_audit: List[int] = []
    try:
        from_audit = buscar_pendentes_audit(conn, limit)
    except Exception as exc:
        logger.error(
            "detail: erro ao consultar visualizacao_resolvidos.audit_recent_missing (%s)",
            exc,
        )
        from_audit = []

    restantes = max(limit - len(from_audit), 0)
    from_fallback: List[int] = []
    if restantes > 0:
        from_fallback = buscar_pendentes_fallback(conn, restantes)

    pendentes: List[int] = []
    vistos: Set[int] = set()
    for tid in from_audit + from_fallback:
        if tid not in vistos:
            vistos.add(tid)
            pendentes.append(tid)

    return pendentes, set(from_audit)


# ----------------------------------------------------------------------
# Escrita no Postgres
# ----------------------------------------------------------------------
def registrar_404(conn, ticket_ids: List[int]) -> None:
    """
    Registra tickets 404 em visualizacao_resolvidos.audit_ticket_watch.
    Usa só a coluna ticket_id e ignora duplicatas.
    """
    if not ticket_ids:
        return

    sql = """
        INSERT INTO visualizacao_resolvidos.audit_ticket_watch (ticket_id)
        VALUES (%s)
        ON CONFLICT (ticket_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, [(tid,) for tid in ticket_ids], page_size=100)


def remover_do_audit(conn, ticket_ids: List[int]) -> None:
    """Remove IDs processados de audit_recent_missing."""
    if not ticket_ids:
        return

    sql = """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
         WHERE table_name = 'tickets_resolvidos'
           AND ticket_id = ANY(%s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticket_ids,))


def upsert_detalhes(conn, registros: List[Dict]) -> None:
    """Insere/atualiza detalhes em visualizacao_resolvidos.tickets_resolvidos."""
    if not registros:
        return

    sql = """
        INSERT INTO visualizacao_resolvidos.tickets_resolvidos (
            ticket_id,
            status,
            last_update,
            last_resolved_at,
            last_closed_at,
            last_cancelled_at
        )
        VALUES (
            %(ticket_id)s,
            %(status)s,
            %(last_update)s,
            %(last_resolved_at)s,
            %(last_closed_at)s,
            %(last_cancelled_at)s
        )
        ON CONFLICT (ticket_id) DO UPDATE SET
            status           = EXCLUDED.status,
            last_update      = EXCLUDED.last_update,
            last_resolved_at = EXCLUDED.last_resolved_at,
            last_closed_at   = EXCLUDED.last_closed_at,
            last_cancelled_at = EXCLUDED.last_cancelled_at;
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, registros, page_size=100)


def processar_lote(
    conn,
    ticket_ids: List[int],
    ids_do_audit: Set[int],
) -> Tuple[int, int, int]:
    """
    Processa um lote de tickets.

    Retorna:
      (qtd_ok, qtd_falhas_total, qtd_404)
    """
    detalhes: List[Dict] = []
    falhas_404: List[int] = []
    outras_falhas = 0

    for tid in ticket_ids:
        data = movidesk_get_ticket(tid)

        if data is None:
            falhas_404.append(tid)
            continue

        try:
            detalhes.append(
                {
                    "ticket_id": data.get("id") or tid,
                    "status": data.get("status"),
                    "last_update": data.get("lastUpdate"),
                    "last_resolved_at": data.get("resolvedIn"),
                    "last_closed_at": data.get("closedIn"),
                    "last_cancelled_at": data.get("canceledIn"),
                }
            )
        except Exception as exc:
            logger.warning(
                "detail: erro ao montar registro do ticket %s: %s",
                tid,
                exc,
            )
            outras_falhas += 1

    # grava no banco
    upsert_detalhes(conn, detalhes)
    registrar_404(conn, falhas_404)

    # limpa audit (tanto ok quanto 404)
    ids_a_remover = [tid for tid in ticket_ids if tid in ids_do_audit]
    remover_do_audit(conn, ids_a_remover)

    ok = len(detalhes)
    falhas_total = len(falhas_404) + outras_falhas
    return ok, falhas_total, len(falhas_404)


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> None:
    logger.info("detail: início da execução, limite por ciclo=%s", BATCH_LIMIT)

    with psycopg2.connect(PG_DSN) as conn:
        total_ok = 0
        total_falhas = 0
        total_404 = 0

        while True:
            ticket_ids, ids_do_audit = obter_lote_pendentes(conn, BATCH_LIMIT)

            if not ticket_ids:
                logger.info(
                    "detail: nenhum ticket pendente nem no audit nem no fallback; encerrando."
                )
                break

            logger.info(
                "detail: lote com %s tickets pendentes (do audit=%s).",
                len(ticket_ids),
                len(ids_do_audit),
            )

            ok, falhas, qtd_404 = processar_lote(conn, ticket_ids, ids_do_audit)
            conn.commit()

            total_ok += ok
            total_falhas += falhas
            total_404 += qtd_404

            logger.info(
                "detail: ciclo concluído: ok=%s, falhas=%s (404=%s).",
                ok,
                falhas,
                qtd_404,
            )

            # se o lote veio cheio, provavelmente ainda há mais pendências → roda outro ciclo
            if len(ticket_ids) < BATCH_LIMIT:
                break

        logger.info(
            "detail: processados nesta execução: ok=%s, falhas=%s (404=%s).",
            total_ok,
            total_falhas,
            total_404,
        )


if __name__ == "__main__":
    main()
