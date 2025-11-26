import os
import time
import logging
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras


# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))  # segundos entre chamadas

# Tabela de detalhe (onde ficam status, datas, organização etc.)
DETAIL_TABLE = "visualizacao_resolvidos.tickets_resolvidos"

# Nome que as triggers gravam em visualizacao_resolvidos.audit_recent_missing.table_name
AUDIT_TABLE_NAME = "tickets_resolvidos"

# Quantos tickets por rodada
BATCH_SIZE = int(os.getenv("DETAIL_BATCH_SIZE", "100"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# --------------------------------------------------------------------
# Helpers gerais
# --------------------------------------------------------------------

def get_db_conn():
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não configurado")
    return psycopg2.connect(NEON_DSN)


def movidesk_get(path, params=None):
    """Chamada simples à API Movidesk com throttle."""
    if params is None:
        params = {}

    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    params = dict(params)
    params["token"] = API_TOKEN

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    time.sleep(THROTTLE)
    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def parse_utc(dt_str):
    """Converte string ISO da API em datetime com timezone UTC."""
    if not dt_str:
        return None
    try:
        dt_str = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# --------------------------------------------------------------------
# Datas de resolução / fechamento
# --------------------------------------------------------------------

def compute_resolution_dates(ticket: dict):
    """
    Define last_resolved_at e last_closed_at usando os campos nativos:

    - baseStatus = Canceled
        last_resolved_at = resolvedIn (ou closedIn)
        last_closed_at   = NULL

    - baseStatus = Resolved
        last_resolved_at = resolvedIn (ou closedIn)
        last_closed_at   = NULL

    - baseStatus = Closed
        last_resolved_at = resolvedIn (se não tiver, usa closedIn)
        last_closed_at   = closedIn

    - Qualquer outro status → ambos NULL
    """
    base_status = (ticket.get("baseStatus") or "").strip()

    resolved_in = parse_utc(ticket.get("resolvedIn"))
    closed_in = parse_utc(ticket.get("closedIn"))

    last_resolved_at = None
    last_closed_at = None

    if base_status == "Canceled":
        last_resolved_at = resolved_in or closed_in
        last_closed_at = None

    elif base_status == "Resolved":
        last_resolved_at = resolved_in or closed_in
        last_closed_at = None

    elif base_status == "Closed":
        last_resolved_at = resolved_in or closed_in
        last_closed_at = closed_in

    else:
        last_resolved_at = None
        last_closed_at = None

    return last_resolved_at, last_closed_at


# --------------------------------------------------------------------
# Mapeamento do ticket da API -> linha da tabela DETAIL_TABLE
# --------------------------------------------------------------------

def safe_get_org(ticket: dict):
    """Extrai organization_id e organization_name do primeiro client, se existir."""
    clients = ticket.get("clients") or []
    if not clients:
        return None, None

    org = (clients[0] or {}).get("organization") or {}
    org_id = org.get("id")
    org_name = org.get("businessName") or org.get("name")
    return org_id, org_name


def safe_get_service_levels(ticket: dict):
    """Extrai até 3 níveis de serviço se existirem."""
    service = ticket.get("service") or {}
    first_level = (service.get("firstLevel") or {}).get("name")
    second_level = (service.get("secondLevel") or {}).get("name")
    third_level = (service.get("thirdLevel") or {}).get("name")
    return first_level, second_level, third_level


def get_custom_field(ticket: dict, field_id_or_label: str):
    """
    Procura um customField pelo id OU pela label.
    Devolve o 'value' se achar.
    """
    for f in ticket.get("customFields") or []:
        if (
            str(f.get("id")) == str(field_id_or_label)
            or (f.get("label") or "").strip() == field_id_or_label
        ):
            return f.get("value")
    return None


def map_ticket_to_detail_row(ticket: dict) -> dict:
    """
    Converte o JSON do ticket da API em um dict com as colunas da tabela DETAIL_TABLE.
    """
    ticket_id = int(ticket["id"])
    status_text = ticket.get("status") or ticket.get("baseStatus")

    last_resolved_at, last_closed_at = compute_resolution_dates(ticket)
    organization_id, organization_name = safe_get_org(ticket)
    service_first_level, service_second_level, service_third_level = safe_get_service_levels(ticket)

    origin = ticket.get("origin")

    # Exemplo de campo adicional (ajuste o identificador conforme seu ambiente)
    adicional_nome = get_custom_field(ticket, "adicional_nome")
    adicional_csat = get_custom_field(ticket, "137641")  # adicional_137641_avaliado_csat

    row = {
        "ticket_id": ticket_id,
        "status_text": status_text,
        "last_resolved_at": last_resolved_at,
        "last_closed_at": last_closed_at,
        "organization_id": organization_id,
        "organization_name": organization_name,
        "service_first_level": service_first_level,
        "service_second_level": service_second_level,
        "service_third_level": service_third_level,
        "adicional_nome": adicional_nome,
        "adicional_137641_avaliado_csat": adicional_csat,
        "origin": origin,
    }

    return row


# --------------------------------------------------------------------
# Busca dos ticket_ids que estão na audit_recent_missing
# --------------------------------------------------------------------

def fetch_missing_ticket_ids(cur, limit: int):
    """
    Busca os ticket_id da audit_recent_missing para essa tabela,
    pegando sempre os runs mais novos primeiro.

    Usa run_id (não existe run_at nessa tabela).
    """
    cur.execute(
        """
        SELECT ticket_id
        FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = %s
        GROUP BY ticket_id
        ORDER BY max(run_id) DESC, ticket_id DESC
        LIMIT %s
        """,
        (AUDIT_TABLE_NAME, limit),
    )
    rows = cur.fetchall()
    return [r[0] for r in rows]


def delete_from_missing(cur, ticket_ids):
    """Remove os tickets processados da audit_recent_missing."""
    if not ticket_ids:
        return 0

    cur.execute(
        """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = %s
          AND ticket_id = ANY(%s)
        """,
        (AUDIT_TABLE_NAME, ticket_ids),
    )
    return cur.rowcount


# --------------------------------------------------------------------
# UPSERT na tabela de detalhe
# --------------------------------------------------------------------

def upsert_detail_rows(cur, rows):
    """
    Faz UPSERT na DETAIL_TABLE usando ticket_id como chave única.
    """
    if not rows:
        return 0

    columns = list(rows[0].keys())

    col_list = ", ".join(columns)
    placeholders = ", ".join([f"%({c})s" for c in columns])
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != "ticket_id"])

    sql = f"""
        INSERT INTO {DETAIL_TABLE} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (ticket_id) DO UPDATE SET
            {updates}
    """

    psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    return len(rows)


# --------------------------------------------------------------------
# Fluxo principal
# --------------------------------------------------------------------

def process_batch(cur, ticket_ids):
    """
    Para cada ticket_id, chama a API, mapeia os campos e prepara as linhas.
    """
    rows = []
    not_found_ids = []

    for ticket_id in ticket_ids:
        ticket = movidesk_get(f"tickets/{ticket_id}")
        if ticket is None:
            logging.warning("ticket %s não encontrado na API (404).", ticket_id)
            not_found_ids.append(ticket_id)
            continue

        row = map_ticket_to_detail_row(ticket)

        # Debug: mostra quais campos ainda ficaram NULL
        null_fields = [k for k, v in row.items() if v is None]
        if null_fields:
            logging.debug(
                "ticket %s campos ainda NULL depois do map_row: %s",
                ticket_id,
                null_fields,
            )

        rows.append(row)

    updated = upsert_detail_rows(cur, rows)
    return updated, not_found_ids


def main():
    logging.info("Iniciando sync_resolved_detail (detail)...")

    with get_db_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        ticket_ids = fetch_missing_ticket_ids(cur, BATCH_SIZE)
        logging.info(
            "Reprocessando %d tickets da audit_recent_missing (mais novos primeiro): %s",
            len(ticket_ids),
            ticket_ids,
        )

        if not ticket_ids:
            logging.info("Nenhum ticket para processar, encerrando.")
            return

        updated, not_found = process_batch(cur, ticket_ids)
        logging.info("UPSERT detail: %d linhas atualizadas.", updated)

        # Remove todos da missing (inclui 404 pra não ficar em loop)
        deleted = delete_from_missing(cur, ticket_ids)
        logging.info("DELETE MISSING: %d", deleted)

        conn.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Erro ao executar sync_resolved_detail: %s", e)
        print(f"[FATAL] Erro ao executar sync_resolved_detail: {e}")
        raise
