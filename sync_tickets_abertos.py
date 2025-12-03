import os
import time
import json
import logging
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import Json

# -------------------------
# Configuração básica
# -------------------------

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

# Quantos tickets de cada vez
BATCH_SIZE = int(os.getenv("DETAIL_OPEN_RAW_LIMIT", "20"))
# Timeout de cada chamada ao Movidesk
HTTP_TIMEOUT = int(os.getenv("MOVIDESK_TIMEOUT", "30"))
# Pausa entre chamadas pra não agredir a API
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] abertos_raw: %(message)s",
)
log = logging.getLogger("abertos_raw")


# -------------------------
# Cliente Movidesk
# -------------------------

class MovideskClient:
    def __init__(self, token: str, timeout: int = 30):
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "movidesk-sync/tickets_abertos_raw"})

    def _request(self, path: str, params: dict | None = None):
        url = f"{API_BASE}{path}"
        p = dict(params or {})
        p["token"] = self.token

        resp = requests.get(url, params=p, timeout=self.timeout)

        # pequenos retries pra 429/5xx se quiser, mas por enquanto 1 tentativa
        if resp.status_code == 200:
            data = resp.json()
            return data

        # Se for algo crítico, deixa levantar HTTPError
        resp.raise_for_status()

    def get_ticket(self, ticket_id: int) -> dict | None:
        """
        Busca um ticket específico pelo ID em /tickets?id=...
        """
        params = {
            "id": ticket_id,
            "includeDeletedItems": "true",
        }
        data = self._request("/tickets", params)

        # Movidesk às vezes devolve lista, às vezes objeto
        if isinstance(data, list):
            return data[0] if data else None
        return data


# -------------------------
# Funções de banco
# -------------------------

def ensure_error_columns(conn):
    """
    Garante que a tabela visualizacao_atual.tickets_abertos
    tenha colunas para registrar erro de raw.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            alter table visualizacao_atual.tickets_abertos
            add column if not exists raw_error_at timestamptz
            """
        )
        cur.execute(
            """
            alter table visualizacao_atual.tickets_abertos
            add column if not exists raw_error_msg text
            """
        )
    conn.commit()


def get_pending_open_tickets_for_raw(conn, limit: int):
    """
    Seleciona tickets abertos que precisam ter o RAW atualizado.

    Critérios:
      - raw é NULL OU updated_at > raw_last_update
      - não estão na tabela de tickets mesclados
      - NÃO possuem raw_error_at (ou seja, ainda não falharam)
    """
    sql = """
    select
        t.ticket_id,
        t.updated_at,
        t.raw_last_update
    from visualizacao_atual.tickets_abertos t
    left join visualizacao_resolvidos.tickets_mesclados m
           on m.ticket_id = t.ticket_id
    where
        -- precisa de raw ou foi atualizado depois do último raw
        (
            t.raw is null
            or t.updated_at > coalesce(t.raw_last_update, timestamp '1970-01-01')
        )
        -- não é ticket mesclado
        and m.ticket_id is null
        -- não está marcado com erro de raw
        and (t.raw_error_at is null)
    order by
        t.raw_last_update nulls first,
        t.updated_at,
        t.ticket_id
    limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return rows


def update_ticket_raw(conn, ticket_id: int, ticket_payload: dict):
    """
    Atualiza o raw e a data do último update de raw para o ticket.
    Também zera qualquer flag de erro anterior.
    """
    sql = """
    update visualizacao_atual.tickets_abertos
       set raw            = %s,
           raw_last_update = now(),
           raw_error_at    = null,
           raw_error_msg   = null
     where ticket_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (Json(ticket_payload), ticket_id))
    conn.commit()


def mark_raw_error(conn, ticket_id: int, err_msg: str):
    """
    Marca que houve erro ao tentar buscar/atualizar o RAW desse ticket.

    Assim ele sai da fila de pendentes de raw e o job não fica
    tentando infinitamente o mesmo ticket.
    """
    sql = """
    update visualizacao_atual.tickets_abertos
       set raw_error_at  = now(),
           raw_error_msg = %s
     where ticket_id = %s
    """
    msg = (err_msg or "")[:800]
    with conn.cursor() as cur:
        cur.execute(sql, (msg, ticket_id))
    conn.commit()


# -------------------------
# Loop principal
# -------------------------

def sync_open_raw():
    log.info("Iniciando sync de raw de tickets abertos (batch_size=%s).", BATCH_SIZE)

    client = MovideskClient(TOKEN, timeout=HTTP_TIMEOUT)
    total_ok = 0
    total_fail = 0

    with psycopg2.connect(DSN) as conn:
        ensure_error_columns(conn)

        while True:
            pending = get_pending_open_tickets_for_raw(conn, BATCH_SIZE)
            if not pending:
                log.info("Nenhum ticket aberto pendente de raw nesta rodada.")
                break

            log.info(
                "Encontrados %d tickets pendentes de raw nesta rodada.",
                len(pending),
            )

            for idx, (ticket_id, updated_at, raw_last_update) in enumerate(
                pending, start=1
            ):
                log.info(
                    "Processando raw de ticket aberto %d/%d "
                    "(ID=%s, updated_at=%s, raw_last_update=%s)",
                    idx,
                    len(pending),
                    ticket_id,
                    updated_at,
                    raw_last_update,
                )

                try:
                    log.info(
                        "Tentando buscar ticket %s em /tickets (id=...)",
                        ticket_id,
                    )
                    ticket = client.get_ticket(ticket_id)

                    if not ticket:
                        # Não faz sentido ficar tentando infinito num ticket que vem vazio
                        msg = "Ticket vazio ou não encontrado na API Movidesk"
                        log.error(
                            "Ticket %s retornou vazio na API. Marcando erro e seguindo. (%s)",
                            ticket_id,
                            msg,
                        )
                        mark_raw_error(conn, ticket_id, msg)
                        total_fail += 1
                        continue

                    # Atualiza RAW no banco
                    update_ticket_raw(conn, ticket_id, ticket)
                    total_ok += 1
                    log.info(
                        "Raw do ticket aberto %s atualizado com sucesso.",
                        ticket_id,
                    )

                except requests.HTTPError as e:
                    # Erro HTTP específico (404, 500, etc)
                    log.error(
                        "Erro HTTP ao buscar ticket_id=%s: %s",
                        ticket_id,
                        e,
                        exc_info=True,
                    )
                    mark_raw_error(conn, ticket_id, f"HTTPError: {e}")
                    total_fail += 1

                except Exception as e:
                    # Timeout (que é o caso do 266909) cai aqui também
                    log.error(
                        "Erro inesperado ao buscar ticket_id=%s: %s",
                        ticket_id,
                        e,
                        exc_info=True,
                    )
                    mark_raw_error(conn, ticket_id, f"Exception: {e}")
                    total_fail += 1

                time.sleep(THROTTLE)

    log.info(
        "Sync raw concluído. Sucesso=%d, Falhas=%d.",
        total_ok,
        total_fail,
    )


if __name__ == "__main__":
    sync_open_raw()
