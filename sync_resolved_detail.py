#!/usr/bin/env python3
"""
detail.py – debug de um único ticket Movidesk (ID fixo 295896)

Objetivo: confirmar qual combinação de endpoint/parâmetros retorna o ticket
e ver o JSON bruto que a API devolve.
"""

import os
import sys
import json
import logging
from typing import Any, Dict, Optional

import requests

# ---------------------------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------------------------

BASE_URL = "https://api.movidesk.com/public/v1"
TICKET_ID = int(os.environ.get("TICKET_ID", "295896"))  # permite sobrescrever se quiser
LOGGER_NAME = "detail"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(LOGGER_NAME)


def get_token() -> str:
    token = os.environ.get("MOVIDESK_TOKEN")
    if not token:
        log.error("Variável de ambiente MOVIDESK_TOKEN não encontrada.")
        sys.exit(1)
    return token


# ---------------------------------------------------------------------------
# Chamadas à API
# ---------------------------------------------------------------------------

def call_api(url: str, params: Dict[str, Any]) -> requests.Response:
    """Faz uma chamada GET logando a URL final e o status."""
    resp = requests.get(url, params=params, timeout=30)
    log.info("GET %s -> %s", resp.url, resp.status_code)
    return resp


def fetch_single_ticket_from_tickets(token: str, ticket_id: int) -> Optional[Dict[str, Any]]:
    """
    Tenta buscar o ticket como UM ÚNICO TICKET na rota /tickets,
    usando exatamente o formato do manual:

      GET /tickets?token=...&id=1

    includeDeletedItems=true é só pra garantir que não sumiu porque foi deletado.
    """
    url = f"{BASE_URL}/tickets"
    params = {
        "token": token,
        "id": ticket_id,
        "includeDeletedItems": "true",
    }

    resp = call_api(url, params)

    if resp.status_code == 404:
        log.warning("Ticket %s não encontrado em /tickets (404).", ticket_id)
        return None

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        log.error("Erro HTTP em /tickets: %s – corpo: %s", e, resp.text[:500])
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        log.error("Resposta de /tickets não é JSON válido: %s", resp.text[:500])
        return None

    # A API pode devolver um objeto ou uma lista com 1 item.
    if isinstance(data, list):
        if not data:
            log.warning("Lista vazia em /tickets para id=%s.", ticket_id)
            return None
        ticket = data[0]
    elif isinstance(data, dict):
        ticket = data
    else:
        log.error("Formato inesperado em /tickets: %r", type(data))
        return None

    if str(ticket.get("id")) != str(ticket_id):
        log.warning(
            "Ticket retornado por /tickets tem id=%s (esperado=%s).",
            ticket.get("id"),
            ticket_id,
        )

    return ticket


def fetch_single_ticket_from_tickets_past(token: str, ticket_id: int) -> Optional[Dict[str, Any]]:
    """
    Tenta buscar o ticket na rota /tickets/past usando OData:

      GET /tickets/past?token=...&$select=...&$filter=id eq 295896

    Isso segue o padrão do manual de Tickets/Past (token + $select + filtros).
    """
    url = f"{BASE_URL}/tickets/past"
    # seleciono alguns campos principais; se precisar de todos, pode por "*"
    select_fields = [
        "id",
        "protocol",
        "status",
        "type",
        "origin",
        "createdDate",
        "lastUpdate",
        "resolvedIn",
        "closedIn",
    ]
    params = {
        "token": token,
        "$select": ",".join(select_fields),
        "$filter": f"id eq {ticket_id}",
    }

    resp = call_api(url, params)

    if resp.status_code == 404:
        log.warning("Ticket %s não encontrado em /tickets/past (404).", ticket_id)
        return None

    # 400 costuma ser "filtro ou parâmetro inválido"
    if resp.status_code == 400:
        log.error("Erro 400 em /tickets/past – verifique se os parâmetros batem com o manual.")
        log.error("Corpo da resposta: %s", resp.text[:500])
        return None

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        log.error("Erro HTTP em /tickets/past: %s – corpo: %s", e, resp.text[:500])
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        log.error("Resposta de /tickets/past não é JSON válido: %s", resp.text[:500])
        return None

    if not isinstance(data, list):
        log.error("Formato inesperado em /tickets/past (esperado lista): %r", type(data))
        return None

    if not data:
        log.warning("Lista vazia em /tickets/past para id=%s.", ticket_id)
        return None

    ticket = data[0]
    if str(ticket.get("id")) != str(ticket_id):
        log.warning(
            "Ticket retornado por /tickets/past tem id=%s (esperado=%s).",
            ticket.get("id"),
            ticket_id,
        )

    return ticket


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------

def main() -> None:
    token = get_token()
    log.info("Iniciando debug de detalhe para ticket_id=%s", TICKET_ID)

    # 1) Tenta pelo /tickets (modo 'único ticket' do manual)
    log.info("Tentando buscar ticket %s em /tickets (id=...)", TICKET_ID)
    ticket = fetch_single_ticket_from_tickets(token, TICKET_ID)

    # 2) Se não achar, tenta /tickets/past com $filter
    if ticket is None:
        log.info("Não achou em /tickets. Tentando em /tickets/past com $filter=id eq ...")
        ticket = fetch_single_ticket_from_tickets_past(token, TICKET_ID)

    if ticket is None:
        log.error("Ticket %s não foi encontrado em nenhum método testado.", TICKET_ID)
        sys.exit(1)

    # 3) Se achou, imprime alguns campos-chave e o JSON completo.
    log.info("Ticket %s encontrado. Campos principais:", TICKET_ID)
    for field in [
        "id",
        "protocol",
        "status",
        "type",
        "origin",
        "createdDate",
        "lastUpdate",
        "resolvedIn",
        "closedIn",
    ]:
        if field in ticket:
            log.info("  %s = %r", field, ticket[field])

    print("\n======= JSON COMPLETO DO TICKET =======")
    print(json.dumps(ticket, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
