import os
import time
import logging
from typing import Any, Dict, List, Optional

import requests
import psycopg2
import psycopg2.extras

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN = os.getenv("NEON_DSN")

PAGE_SIZE = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

# ID do campo adicional no Movidesk para "avaliado CSAT"
CSAT_FIELD_ID = 137641

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN nos secrets.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tickets_abertos_index")

session = requests.Session()
session.headers.update({"Accept": "application/json"})


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def req(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 90) -> Any:
    """Request simples com retry para 429/503/5xx básicos."""
    p = dict(params or {})
    # token SEMPRE no querystring
    p["token"] = TOKEN

    while True:
        r = session.get(url, params=p, timeout=timeout)

        # Retry para limites / instabilidades
        if r.status_code in (429, 500, 502, 503, 504):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("HTTP %s em %s, aguardando %ss e tentando de novo…", r.status_code, url, wait)
            time.sleep(wait)
            continue

        if r.status_code == 404:
            return []

        r.raise_for_status()
        if not r.text:
            return []
        try:
            return r.json()
        except ValueError:
            logger.error("Resposta não é JSON. Status=%s, texto=%r", r.status_code, r.text[:200])
            raise


# ---------------------------------------------------------------------------
# Helpers de normalização
# ---------------------------------------------------------------------------

def norm_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def norm_ts(value: Any) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s


# ---------------------------------------------------------------------------
# Fetch de tickets abertos na API Movidesk
# ---------------------------------------------------------------------------

def fetch_open_tickets() -> List[Dict[str, Any]]:
    """
    Busca todos os tickets com baseStatus em ('New','InAttendance','Stopped').
    Retorna a lista crua vinda da API.
    """
    url = f"{API_BASE}/tickets"

    # baseStatus aberto
    fil = "(baseStatus eq 'New' or baseStatus eq 'InAttendance' or baseStatus eq 'Stopped')"

    # campos simples
    select_fields = [
        "id",
        "subject",
        "type",
        "status",
        "baseStatus",
        "ownerTeam",
        "serviceFirstLevel",
        "origin",
        "createdDate",
        "lastUpdate",
    ]
    sel = ",".join(select_fields)

    # nav properties / objetos complexos
    # - clients + organization (empresa)
    # - owner (agente responsável)
    # - customFieldValues (para CSAT)
    expand = "clients($expand=organization),owner,customFieldValues"

    items: List[Dict[str, Any]] = []
    skip = 0

    logger.info("Iniciando sync rápido de tickets abertos (index enriquecido).")

    while True:
        params = {
            "$select": sel,
            "$expand": expand,
            "$filter": fil,
            "$orderby": "lastUpdate asc",
            "$top": PAGE_SIZE,
            "$skip": skip,
        }

        page = req(url, params=params) or []
        if not isinstance(page, list):
            logger.warning("Resposta inesperada da API (não é lista): %r", type(page))
            break

        logger.info("Página com %s tickets (acumulado %s)", len(page), len(items) + len(page))
        items.extend(page)

        if len(page) < PAGE_SIZE:
            break

        skip += len(page)
        time.sleep(THROTTLE)

    logger.info("Total de tickets abertos retornados: %s", len(items))
    return items


# ---------------------------------------------------------------------------
# Mapeamento API -> colunas da tabela
# ---------------------------------------------------------------------------

def extract_empresa_from_clients(ticket: Dict[str, Any]) -> (Optional[str], Optional[str]):
    clients = ticket.get("clients") or []
    if not isinstance(clients, list) or not clients:
        return None, None

    # prioriza quem tem organization
    for c in clients:
        org = (c or {}).get("organization") or {}
        if org.get("id") is not None:
            return str(org.get("id")), org.get("businessName")

    return None, None


def extract_owner(ticket: Dict[str, Any]) -> (Optional[int], Optional[str]):
    owner = ticket.get("owner") or {}
    if not isinstance(owner, dict):
        return None, None

    agent_id = norm_int(owner.get("id"))
    responsavel = owner.get("businessName")
    return agent_id, responsavel


def extract_csat(ticket: Dict[str, Any]) -> Optional[str]:
    """Procura o customFieldValues com customFieldId = CSAT_FIELD_ID e retorna o value (ou None)."""
    cfs = ticket.get("customFieldValues") or []
    if not isinstance(cfs, list):
        return None

    for cf in cfs:
        if not isinstance(cf, dict):
            continue
        # alguns ambientes usam 'customFieldId', outros 'customField' com id aninhado.
        cf_id = cf.get("customFieldId")
        try:
            cf_id = int(cf_id) if cf_id is not None else None
        except Exception:
            cf_id = None

        if cf_id == CSAT_FIELD_ID:
            # prioridade para 'value'; se não tiver, tenta pegar primeiro item.
            if cf.get("value") not in (None, ""):
                return str(cf["value"])
            items = cf.get("items") or []
            if isinstance(items, list) and items:
                item0 = items[0] or {}
                # o nome pode estar em 'customFieldItem'
                val = item0.get("customFieldItem") or item0.get("value")
                if val not in (None, ""):
                    return str(val)
    return None


def map_ticket(ticket: Dict[str, Any]) -> Dict[str, Any]:
    empresa_id, empresa_nome = extract_empresa_from_clients(ticket)
    agent_id, responsavel = extract_owner(ticket)
    csat_val = extract_csat(ticket)

    return {
        "id": norm_int(ticket.get("id")),
        "subject": ticket.get("subject"),
        "type": ticket.get("type"),
        "status": ticket.get("status"),
        "base_status": ticket.get("baseStatus"),
        "owner_team": ticket.get("ownerTeam"),
        "service_first_level": ticket.get("serviceFirstLevel"),
        "created_date": norm_ts(ticket.get("createdDate")),
        "last_update": norm_ts(ticket.get("lastUpdate")),
        # contagem fica com default 1 no banco
        "responsavel": responsavel,
        "empresa_cod_ref_adicional": None,  # será preenchido via JOIN com visualizacao_empresa.empresas
        "agent_id": agent_id,
        "empresa_id": empresa_id,
        "empresa_nome": empresa_nome,
        "adicional_137641_avaliado_csat": csat_val,
        "origin": ticket.get("origin"),
    }


# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------

def ensure_table(conn) -> None:
    """Garante schema, tabela e colunas necessárias."""
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_atual")

        cur.execute(
            """
            create table if not exists visualizacao_atual.movidesk_tickets_abertos (
                id integer primary key
            )
            """
        )

        cur.execute(
            """
            alter table visualizacao_atual.movidesk_tickets_abertos
                add column if not exists subject                     varchar(350),
                add column if not exists type                        smallint,
                add column if not exists status                      varchar(128),
                add column if not exists base_status                 varchar(128),
                add column if not exists owner_team                  varchar(128),
                add column if not exists service_first_level         varchar(102),
                add column if not exists created_date                timestamptz,
                add column if not exists last_update                 timestamptz,
                add column if not exists contagem                    integer default 1,
                add column if not exists responsavel                 varchar(255),
                add column if not exists empresa_cod_ref_adicional   varchar(64),
                add column if not exists agent_id                    bigint,
                add column if not exists empresa_id                  text,
                add column if not exists empresa_nome                text,
                add column if not exists adicional_137641_avaliado_csat text,
                add column if not exists origin                      smallint
            """
        )

    conn.commit()


def upsert_open_tickets(conn, rows: List[Dict[str, Any]]) -> int:
    """
    Trunca a tabela e insere todos os tickets abertos.
    Usa INSERT .. ON CONFLICT só por segurança/idempotência.
    """
    if not rows:
        logger.info("Nenhum ticket para inserir.")
        with conn.cursor() as cur:
            cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        conn.commit()
        return 0

    # ordem das colunas para insert
    cols = [
        "id",
        "subject",
        "type",
        "status",
        "base_status",
        "owner_team",
        "service_first_level",
        "created_date",
        "last_update",
        # contagem fica de fora (default 1)
        "responsavel",
        "empresa_cod_ref_adicional",
        "agent_id",
        "empresa_id",
        "empresa_nome",
        "adicional_137641_avaliado_csat",
        "origin",
    ]

    # filtra só tickets com id não-nulo
    payload = [ {c: r.get(c) for c in cols} for r in rows if r.get("id") is not None ]
    if not payload:
        logger.warning("Todos os tickets retornaram id nulo. Nada será inserido.")
        return 0

    insert_cols = ", ".join(cols)
    placeholders = ", ".join([f"%({c})s" for c in cols])
    update_set = ", ".join([f"{c} = excluded.{c}" for c in cols if c != "id"])

    sql = f"""
        insert into visualizacao_atual.movidesk_tickets_abertos ({insert_cols})
        values ({placeholders})
        on conflict (id) do update set
            {update_set}
    """

    with conn.cursor() as cur:
        # Zera a tabela antes de recarregar os abertos
        cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        psycopg2.extras.execute_batch(cur, sql, payload, page_size=200)

    conn.commit()
    logger.info("Inseridos %s tickets na tabela movidesk_tickets_abertos.", len(payload))
    return len(payload)


def enrich_coderef_from_empresas(conn) -> None:
    """
    Preenche empresa_cod_ref_adicional via join com visualizacao_empresa.empresas.codereferenceadditional.
    Se o schema/tabela não existir, só loga um aviso e segue.
    """
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                update visualizacao_atual.movidesk_tickets_abertos t
                   set empresa_cod_ref_adicional = e.codereferenceadditional
                  from visualizacao_empresa.empresas e
                 where e.id = t.empresa_id
                """
            )
            logger.info("Atualização de empresa_cod_ref_adicional concluída (linhas afetadas: %s).", cur.rowcount)
        except psycopg2.Error as e:
            # se schema/tabela não existir, não queremos quebrar o job
            logger.warning(
                "Não foi possível atualizar empresa_cod_ref_adicional (schema visualizacao_empresa.empresas?). Erro: %s",
                e,
            )
    conn.commit()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    tickets_raw = fetch_open_tickets()
    mapped_rows = [map_ticket(t) for t in tickets_raw if isinstance(t, dict)]

    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        upsert_open_tickets(conn, mapped_rows)
        enrich_coderef_from_empresas(conn)

    logger.info("Sync de tickets abertos (index) finalizado com sucesso.")


if __name__ == "__main__":
    main()
