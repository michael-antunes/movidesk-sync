import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras

# --------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------
API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))  # segundos entre requisições
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

# Campos que queremos inspecionar quando ficam NULL
CRITICAL_FIELDS = [
    "organization_id",
    "organization_name",
    "service_second_level",
    "service_third_level",
    "adicional_nome",
    "last_resolved_at",
    "last_closed_at",
]

# ID do campo adicional "Nome" = 29077
ADICIONAL_NOME_ID = 29077


# --------------------------------------------------------------------
# Helpers genéricos
# --------------------------------------------------------------------
def get_connection():
    if not NEON_DSN:
        raise RuntimeError("NEON_DSN não configurado")
    conn = psycopg2.connect(NEON_DSN, sslmode="require")
    conn.autocommit = False
    return conn


def movidesk_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    if params is None:
        params = {}

    params = dict(params)
    params["token"] = API_TOKEN

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code == 404:
        # ticket não existe mais
        return {}
    resp.raise_for_status()
    return resp.json()


def normalize(text: Optional[str]) -> str:
    return (text or "").strip().lower()


# --------------------------------------------------------------------
# Cálculo de datas de resolução / fechamento
# --------------------------------------------------------------------
def compute_resolution_dates(ticket: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Calcula last_resolved_at e last_closed_at a partir da lista de actions do ticket.

    Regras:
      - resolved_at: primeira vez que o status vira 'Resolvido' OU 'Cancelado'
      - closed_at  : última vez que o status vira 'Fechado'
    """
    resolved_at: Optional[str] = None
    closed_at: Optional[str] = None

    actions = ticket.get("actions") or []
    if not isinstance(actions, list):
        return resolved_at, closed_at

    # Ordena pela data da ação
    def _created(a):
        return a.get("createdDate") or ""

    actions_sorted = sorted(actions, key=_created)

    for action in actions_sorted:
        created = action.get("createdDate")
        if not created:
            continue

        status_str = normalize(action.get("status") or action.get("statusName"))

        # 1) Resolução (Resolvido ou Cancelado)
        if ("resolvido" in status_str or "cancelado" in status_str) and resolved_at is None:
            resolved_at = created

        # 2) Fechamento
        if "fechado" in status_str:
            # pega sempre a última vez que virou Fechado
            closed_at = created

    return resolved_at, closed_at


# --------------------------------------------------------------------
# Extração de campos do ticket
# --------------------------------------------------------------------
def extract_adicional_nome(ticket: Dict[str, Any]) -> Optional[str]:
    """
    Campo adicional "Nome" (id 29077) mapeado para adicional_nome.
    A API do Movidesk pode devolver como 'customFieldValues' ou 'customFields'.
    """
    for key in ("customFieldValues", "customFields", "additionalFields"):
        fields = ticket.get(key)
        if not isinstance(fields, list):
            continue

        for f in fields:
            try:
                fid = f.get("id") or f.get("fieldId")
                if isinstance(fid, str) and fid.isdigit():
                    fid = int(fid)
                if fid == ADICIONAL_NOME_ID:
                    # value / fieldValue / texto
                    for vk in ("value", "fieldValue", "text", "displayValue"):
                        if f.get(vk):
                            return str(f[vk]).strip()
            except Exception:
                # qualquer problema aqui, ignora e tenta o próximo
                continue
    return None


def extract_organization(ticket: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Tenta pegar organization_id e organization_name.
    """
    org = (
        ticket.get("organization")
        or ticket.get("ownerOrganization")
        or ticket.get("clientOrganization")
    )

    if not isinstance(org, dict):
        return None, None

    org_id = org.get("id") or org.get("organizationId")
    org_name = org.get("businessName") or org.get("name") or org.get("fantasyName")

    if org_id is not None:
        org_id = str(org_id)

    if org_name is not None:
        org_name = str(org_name)

    return org_id, org_name


def extract_service_levels(ticket: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrai serviço 2º e 3º nível (quando existirem).
    A estrutura pode variar, então vamos tentar alguns caminhos.
    """
    service = ticket.get("service") or {}

    second = service.get("secondLevel") or service.get("serviceSecondLevel")
    third = service.get("thirdLevel") or service.get("serviceThirdLevel")

    # alguns ambientes usam campos flat no ticket
    if not second:
        second = ticket.get("serviceSecondLevel")
    if not third:
        third = ticket.get("serviceThirdLevel")

    if isinstance(second, dict):
        second = second.get("name") or second.get("description")
    if isinstance(third, dict):
        third = third.get("name") or third.get("description")

    second = str(second).strip() if second else None
    third = str(third).strip() if third else None

    return second, third


def map_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o JSON do ticket em uma linha para o tickets_resolvidos.
    """
    ticket_id = ticket.get("id") or ticket.get("ticketId")
    if ticket_id is None:
        raise ValueError("Ticket sem id")

    status = ticket.get("status") or ""
    origin = ticket.get("origin")

    # Campos base
    row: Dict[str, Any] = {
        "ticket_id": int(ticket_id),
        "status": str(status),
        "origin": str(origin) if origin is not None else None,
        "last_resolved_at": None,
        "last_closed_at": None,
        "organization_id": None,
        "organization_name": None,
        "service_second_level": None,
        "service_third_level": None,
        "adicional_nome": None,
    }

    # Organização
    org_id, org_name = extract_organization(ticket)
    row["organization_id"] = org_id
    row["organization_name"] = org_name

    # Serviços 2º / 3º nível
    second, third = extract_service_levels(ticket)
    row["service_second_level"] = second
    row["service_third_level"] = third

    # Campo adicional "Nome"
    row["adicional_nome"] = extract_adicional_nome(ticket)

    # Datas de resolução / fechamento
    resolved_at, closed_at = compute_resolution_dates(ticket)
    row["last_resolved_at"] = resolved_at
    row["last_closed_at"] = closed_at

    # Loga campos críticos ainda NULL (pra debug)
    null_fields = [f for f in CRITICAL_FIELDS if row.get(f) is None]
    if null_fields:
        print(
            f"[DEBUG] ticket {row['ticket_id']} campos ainda NULL depois do map_row: {null_fields}"
        )

    return row


# --------------------------------------------------------------------
# Banco: leitura dos tickets em missing
# --------------------------------------------------------------------
def fetch_missing_ticket_ids(cur, limit: int) -> List[int]:
    """
    Busca até 'limit' tickets para reprocessar a partir de audit_recent_missing.
    """
    cur.execute(
        """
        SELECT ticket_id
        FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = 'tickets_resolvidos'
        ORDER BY id DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return [r[0] for r in rows]


def delete_from_missing(cur, ticket_ids: List[int]) -> int:
    if not ticket_ids:
        return 0

    cur.execute(
        """
        DELETE FROM visualizacao_resolvidos.audit_recent_missing
        WHERE table_name = 'tickets_resolvidos'
          AND ticket_id = ANY(%s)
        """,
        (ticket_ids,),
    )
    return cur.rowcount


# --------------------------------------------------------------------
# UPSERT no detalhe
# --------------------------------------------------------------------
def upsert_detail(cur, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    columns = [
        "ticket_id",
        "status",
        "origin",
        "last_resolved_at",
        "last_closed_at",
        "organization_id",
        "organization_name",
        "service_second_level",
        "service_third_level",
        "adicional_nome",
    ]

    values = [[row.get(col) for col in columns] for row in rows]

    sql = f"""
        INSERT INTO visualizacao_resolvidos.tickets_resolvidos
        ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            status              = EXCLUDED.status,
            origin              = EXCLUDED.origin,
            last_resolved_at    = EXCLUDED.last_resolved_at,
            last_closed_at      = EXCLUDED.last_closed_at,
            organization_id     = EXCLUDED.organization_id,
            organization_name   = EXCLUDED.organization_name,
            service_second_level = EXCLUDED.service_second_level,
            service_third_level  = EXCLUDED.service_third_level,
            adicional_nome       = EXCLUDED.adicional_nome;
    """

    psycopg2.extras.execute_values(cur, sql, values, page_size=100)
    return len(rows)


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            ticket_ids = fetch_missing_ticket_ids(cur, BATCH_SIZE)

        if not ticket_ids:
            print("Nenhum ticket na audit_recent_missing para reprocessar, saindo.")
            return

        print(
            f"Reprocessando {len(ticket_ids)} tickets da audit_recent_missing "
            f"(mais novos primeiro): {ticket_ids}"
        )

        rows: List[Dict[str, Any]] = []

        for i, ticket_id in enumerate(ticket_ids, start=1):
            try:
                ticket = movidesk_get(f"tickets/{ticket_id}", params={"include": "actions,service,organization,customFieldValues"})
                if not ticket:
                    # 404 ou algo assim: ainda assim vamos remover do missing
                    print(f"[WARN] ticket {ticket_id} não encontrado na API (404).")
                    continue

                row = map_row(ticket)
                rows.append(row)

            except Exception as e:
                print(f"[ERROR] Falha ao processar ticket {ticket_id}: {e}")

            # respeita throttling
            if THROTTLE > 0 and i < len(ticket_ids):
                time.sleep(THROTTLE)

        with conn.cursor() as cur:
            updated = upsert_detail(cur, rows)
            print(f"UPSERT detail: {updated} linhas atualizadas.")

            deleted = delete_from_missing(cur, ticket_ids)
            print(f"DELETE MISSING: {deleted}")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[FATAL] Erro ao executar sync_resolved_detail: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
