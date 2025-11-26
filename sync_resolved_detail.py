import os
import time
import logging
from typing import Any, Dict, List, Optional

import requests
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN")

BATCH_SIZE = int(os.getenv("SYNC_RESOLVED_DETAIL_BATCH", "100"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sync_resolved_detail")


# ---------------------------------------------------------------------------
# Helpers de API
# ---------------------------------------------------------------------------

def md_get(path: str, params: Optional[Dict[str, Any]] = None, ok_404: bool = False) -> Any:
    """
    Chama a API do Movidesk com tratamento básico de erro.
    Mantém a mesma assinatura utilizada nos outros scripts.
    """
    if API_TOKEN is None:
        raise RuntimeError("MOVIDESK_TOKEN não configurado")

    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    query = dict(params or {})
    query["token"] = API_TOKEN

    resp = requests.get(url, params=query, timeout=30)
    if resp.status_code == 404 and ok_404:
        return None
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Helpers de banco / auditoria
# ---------------------------------------------------------------------------

def get_pending_ids(conn, limit: int) -> List[int]:
    """
    Busca os ticket_id que estão em audit_recent_missing para a tabela tickets_resolvidos.

    A trigger nova joga tudo pra missing, então:
    - filtramos só por table_name = 'tickets_resolvidos'
    - ordenamos pelos runs mais recentes
    - deduplicamos por ticket_id em Python
    """
    sql = """
        SELECT
            m.ticket_id,
            r.run_at
        FROM audit_recent_missing m
        JOIN audit_recent_run r
          ON r.id = m.run_id
        WHERE m.table_name = 'tickets_resolvidos'
        ORDER BY r.run_at DESC, m.ticket_id DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    seen = set()
    pending: List[int] = []
    for ticket_id, _run_at in rows:
        if ticket_id not in seen:
            seen.add(ticket_id)
            pending.append(ticket_id)

    return pending


def register_ticket_failure(conn, ticket_id: int, reason: str) -> None:
    """
    Registra motivo da falha em audit_ticket_watch.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_ticket_watch (ticket_id, source, reason)
            VALUES (%s, 'sync_resolved_detail', %s)
            """,
            (ticket_id, reason),
        )


def mark_still_missing(conn, ticket_id: int, missing_fields: List[str]) -> None:
    """
    Mesmo após chamar a API ainda ficamos sem alguns campos -> registramos em audit_ticket_watch.
    """
    reason = "still_null_fields:" + ",".join(sorted(missing_fields))
    register_ticket_failure(conn, ticket_id, reason)


def delete_processed_from_missing(conn, ids: List[int]) -> None:
    """
    Remove da audit_recent_missing todos os registros da tabela tickets_resolvidos
    para os ticket_id processados com sucesso.
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM audit_recent_missing
            WHERE table_name = 'tickets_resolvidos'
              AND ticket_id = ANY(%s)
            """,
            (ids,),
        )


# ---------------------------------------------------------------------------
# Mapeamento de campos do ticket -> tickets_resolvidos
# ---------------------------------------------------------------------------

def parse_datetime(value: Optional[str]) -> Optional[str]:
    """
    A API já manda os datetimes em ISO 8601; deixamos como string
    e deixamos o psycopg2 converter.
    """
    if not value:
        return None
    return value


def extract_custom_field(ticket: Dict[str, Any], field_id: int) -> Optional[str]:
    """
    Busca um customField específico (ex: CSAT 137641) na estrutura retornada.
    """
    customs = ticket.get("customFields") or []
    for cf in customs:
        if cf.get("id") == field_id:
            val = cf.get("value")
            if isinstance(val, str) and not val.strip():
                return None
            return val

    # Alguns ambientes usam 'customFieldValues'
    for cf in ticket.get("customFieldValues") or []:
        if cf.get("customFieldId") == field_id:
            val = cf.get("value")
            if isinstance(val, str) and not val.strip():
                return None
            return val

    return None


def build_detail_row(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta o dicionário de campos para tickets_resolvidos.

    **FOCO NOS TEMPOS**:
    - Usa resolvedIn / closedIn primeiro;
    - Se vierem nulos, tenta inferir a partir de statusHistories.
    """
    ticket_id = int(ticket["id"])
    status = ticket.get("status")

    # Tempos principais
    resolved_at = parse_datetime(ticket.get("resolvedIn"))
    closed_at = parse_datetime(ticket.get("closedIn"))

    histories = ticket.get("statusHistories") or []

    if not resolved_at:
        for h in histories:
            sb = (h.get("statusBase") or "").lower()
            st = (h.get("status") or "").lower()
            if sb == "resolved" or "resolvido" in st:
                resolved_at = parse_datetime(h.get("changedDate"))
                break

    if not closed_at:
        for h in histories:
            sb = (h.get("statusBase") or "").lower()
            st = (h.get("status") or "").lower()
            if sb == "closed" or "fechado" in st:
                closed_at = parse_datetime(h.get("changedDate"))
                break

    # Organização / hotel
    org = ticket.get("organization") or {}
    organization_id = org.get("id")
    organization_name = org.get("businessName") or org.get("name")

    # Outros campos que já existiam na tabela
    origin = ticket.get("origin")
    adicional_nome = ticket.get("subject")

    # CSAT – pode não existir
    adicional_csat = extract_custom_field(ticket, 137641)

    row = {
        "ticket_id": ticket_id,
        "status": status,
        "last_resolved_at": resolved_at,
        "last_closed_at": closed_at,
        "organization_id": organization_id,
        "organization_name": organization_name,
        "origin": origin,
        "adicional_nome": adicional_nome,
        "adicional_137641_avaliado_csat": adicional_csat,
    }
    return row


def upsert_details(conn, rows: List[Dict[str, Any]]) -> None:
    """
    INSERT/UPDATE em tickets_resolvidos dos campos controlados aqui.
    """
    if not rows:
        return

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO tickets_resolvidos (
                ticket_id,
                status,
                last_resolved_at,
                last_closed_at,
                organization_id,
                organization_name,
                origin,
                adicional_nome,
                adicional_137641_avaliado_csat
            )
            VALUES (
                %(ticket_id)s,
                %(status)s,
                %(last_resolved_at)s,
                %(last_closed_at)s,
                %(organization_id)s,
                %(organization_name)s,
                %(origin)s,
                %(adicional_nome)s,
                %(adicional_137641_avaliado_csat)s
            )
            ON CONFLICT (ticket_id) DO UPDATE SET
                status = EXCLUDED.status,
                last_resolved_at = EXCLUDED.last_resolved_at,
                last_closed_at = EXCLUDED.last_closed_at,
                organization_id = EXCLUDED.organization_id,
                organization_name = EXCLUDED.organization_name,
                origin = EXCLUDED.origin,
                adicional_nome = EXCLUDED.adicional_nome,
                adicional_137641_avaliado_csat = EXCLUDED.adicional_137641_avaliado_csat
            """,
            rows,
            page_size=50,
        )


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Iniciando sync_resolved_detail (detail)...")

    if not DSN:
        raise RuntimeError("NEON_DSN não configurado")

    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False

        pending = get_pending_ids(conn, BATCH_SIZE)
        total_pendentes = len(pending)

        if not pending:
            logger.info(
                "detail: nenhum ticket pendente em audit_recent_missing para tickets_resolvidos."
            )
            return

        logger.info(
            "detail: %d tickets pendentes em audit_recent_missing (limite=%d).",
            total_pendentes,
            BATCH_SIZE,
        )

        detalhes: List[Dict[str, Any]] = []
        ok_ids: List[int] = []
        fail_reasons: Dict[str, int] = {}
        fail_samples: Dict[str, int] = {}

        for tid in pending:
            reason: Optional[str] = None

            try:
                # IMPORTANTE: expand actions/customFields/statusHistories/organization,
                # como na versão antiga que trazia corretamente os tempos.
                data = md_get(
                    f"tickets/{tid}",
                    params={
                        "$expand": "clients,createdBy,owner,actions,customFields,"
                                   "statusHistories,organization"
                    },
                    ok_404=True,
                )
            except requests.HTTPError as e:
                status_code = getattr(e.response, "status_code", None)
                reason = f"http_error_{status_code or 'unknown'}"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue
            except Exception:
                reason = "exception_api"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if data is None:
                reason = "not_found_404"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            if isinstance(data, list):
                if not data:
                    reason = "empty_list"
                    register_ticket_failure(conn, tid, reason)
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    if reason not in fail_samples:
                        fail_samples[reason] = tid
                    continue
                ticket = data[0]
            else:
                ticket = data

            if not ticket.get("id"):
                reason = "missing_id"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            try:
                row = build_detail_row(ticket)
            except Exception:
                reason = "build_row_error"
                register_ticket_failure(conn, tid, reason)
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                if reason not in fail_samples:
                    fail_samples[reason] = tid
                continue

            # Campos críticos de tempo ainda nulos?
            missing_fields = [
                field
                for field in ("last_resolved_at", "last_closed_at")
                if row.get(field) is None
            ]
            if missing_fields:
                logger.debug(
                    "ticket %s campos ainda NULL depois do build_detail_row: %s",
                    tid,
                    missing_fields,
                )
                mark_still_missing(conn, tid, missing_fields)

            detalhes.append(row)
            ok_ids.append(tid)
            time.sleep(THROTTLE)

        total_ok = len(ok_ids)
        total_fail = sum(fail_reasons.values())

        logger.info("detail: processados neste ciclo: ok=%d, falhas=%d.", total_ok, total_fail)

        if fail_reasons:
            logger.info("detail: razões de falha neste ciclo:")
            for r, c in fail_reasons.items():
                sample = fail_samples.get(r)
                logger.info("  - %s: %d tickets (exemplo ticket_id=%s)", r, c, sample)

        if not detalhes:
            logger.info(
                "detail: nenhum ticket com detalhe válido; apenas falhas registradas em audit_ticket_watch."
            )
            conn.commit()
            return

        upsert_details(conn, detalhes)
        delete_processed_from_missing(conn, ok_ids)
        logger.info(
            "detail: %d tickets upsertados em tickets_resolvidos e removidos do missing.",
            total_ok,
        )
        conn.commit()


if __name__ == "__main__":
    main()
