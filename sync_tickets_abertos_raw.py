import os
import json
import time
import logging

import requests
import psycopg2

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

BATCH_SIZE = int(os.getenv("ABERTOS_RAW_BATCH", "20"))
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))
TIMEOUT = int(os.getenv("MOVIDESK_TIMEOUT", "30"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("abertos_raw")


class MovideskClient:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout

    def _request(self, path_or_full, params=None):
        url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full.lstrip('/')}"
        p = dict(params or {})
        p["token"] = TOKEN

        resp = requests.get(url, params=p, timeout=self.timeout)
        # para erros de infra, tenta mais uma vez
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning("Retry em %s por status %s", url, resp.status_code)
            time.sleep(1.5)
            resp = requests.get(url, params=p, timeout=self.timeout)

        resp.raise_for_status()
        data = resp.json()
        return data or []

    def get_ticket(self, ticket_id):
        params = {
            "id": ticket_id,
            "includeDeletedItems": "true",  # aqui pode trazer apagado, pq só queremos o histórico bruto
        }
        data = self._request("tickets", params)
        if isinstance(data, list):
            return data[0] if data else None
        return data


def get_pending_raw_tickets(conn, limit):
    """
    Busca tickets em visualizacao_atual.tickets_abertos que:
      - não são mesclados
      - raw está vazio OU está desatualizado (updated_at > raw_last_update)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select ta.ticket_id,
                   ta.updated_at,
                   ta.raw_last_update
              from visualizacao_atual.tickets_abertos ta
         left join visualizacao_resolvidos.tickets_mesclados tm
                on tm.ticket_id = ta.ticket_id
             where tm.ticket_id is null
               and (ta.raw_last_update is null or ta.updated_at > ta.raw_last_update)
          order by coalesce(ta.raw_last_update, '1970-01-01'::timestamptz),
                   ta.updated_at
             limit %s
            """,
            (limit,),
        )
        return cur.fetchall()


def save_raw(conn, ticket_id, payload):
    """Atualiza o campo raw e raw_last_update para um ticket."""
    raw_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
    with conn.cursor() as cur:
        cur.execute(
            """
            update visualizacao_atual.tickets_abertos
               set raw = %s,
                   raw_last_update = now()
             where ticket_id = %s
            """,
            (raw_json, ticket_id),
        )
    conn.commit()


def sync_open_raw(conn):
    client = MovideskClient()

    logger.info("Iniciando sync de raw de tickets abertos (batch_size=%s).", BATCH_SIZE)

    while True:
        pending = get_pending_raw_tickets(conn, BATCH_SIZE)
        if not pending:
            logger.info("Nenhum ticket pendente de raw nesta rodada.")
            break

        logger.info("Encontrados %s tickets pendentes de raw nesta rodada.", len(pending))

        for idx, (ticket_id, updated_at, raw_last_update) in enumerate(pending, start=1):
            logger.info(
                "Processando raw de ticket aberto %s/%s (ID=%s, updated_at=%s, raw_last_update=%s)",
                idx,
                len(pending),
                ticket_id,
                updated_at,
                raw_last_update,
            )

            logger.info("Tentando buscar ticket %s em /tickets (id=...)", ticket_id)

            try:
                ticket = client.get_ticket(ticket_id)
            except requests.HTTPError as e:
                # Erro HTTP (4xx/5xx) – loga e segue para o próximo
                logger.error(
                    "Erro HTTP ao buscar ticket_id=%s: %s",
                    ticket_id,
                    e,
                )
                continue
            except Exception as e:
                # timeout / erro de rede / qualquer outro erro inesperado
                logger.error(
                    "Erro inesperado ao buscar ticket_id=%s: %s",
                    ticket_id,
                    e,
                )
                # não atualiza raw_last_update -> fica pendente para próxima rodada
                continue

            # Se não veio nada da API, ainda assim marca raw_last_update pra não ficar eterno
            if ticket is None:
                logger.warning(
                    "Ticket %s não retornou dados na API (lista vazia). Marcando como processado sem raw.",
                    ticket_id,
                )

            save_raw(conn, ticket_id, ticket)
            logger.info("Raw do ticket aberto %s atualizado com sucesso.", ticket_id)

            time.sleep(THROTTLE)


def main():
    with psycopg2.connect(DSN) as conn:
        sync_open_raw(conn)


if __name__ == "__main__":
    main()
