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

TOP = int(os.getenv("MOVIDESK_PAGE_SIZE", "500"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN nos secrets.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tickets_abertos_index")

http = requests.Session()
http.headers.update({"Accept": "application/json"})


def req(path_or_url, params=None, timeout=90):
    """GET com retry simples para 429/503."""
    url = path_or_url if path_or_url.startswith("http") else f"{API_BASE}/{path_or_url.lstrip('/')}"
    p = dict(params or {})
    p["token"] = TOKEN

    while True:
        r = http.get(url, params=p, timeout=timeout)
        if r.status_code in (429, 503):
            ra = r.headers.get("retry-after")
            wait = int(ra) if ra and str(ra).isdigit() else 60
            logger.warning("HTTP %s em %s, retry em %ss", r.status_code, url, wait)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json() if r.text else []


def iint(x):
    try:
        s = str(x)
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def norm_ts(x):
    if not x:
        return None
    s = str(x).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    return s


def get_cf_value(ticket, cf_id):
    """Pega valor de customFieldId == cf_id."""
    cfs = ticket.get("customFieldValues") or []
    for cf in cfs:
        try:
            cid = cf.get("customFieldId")
            if int(cid) == int(cf_id):
                v = cf.get("value")
                if isinstance(v, str) or v is None:
                    return v
                return str(v)
        except Exception:
            continue
    return None


def fetch_open_tickets():
    """
    Busca todos os tickets abertos (baseStatus New / InAttendance / Stopped)
    já com clients, owner etc. Não usamos $select pra simplificar
    e garantir que venham os customFieldValues.
    """
    url = "tickets"
    skip = 0
    filtro = "(baseStatus eq 'New' or baseStatus eq 'InAttendance' or baseStatus eq 'Stopped')"
    items = []

    while True:
        page = req(
            url,
            {
                "$filter": filtro,
                "$orderby": "lastUpdate asc",
                "$top": TOP,
                "$skip": skip,
                "includeDeletedItems": "false",
            },
        ) or []

        if not page:
            break

        items.extend(page)
        logger.info("Página com %s tickets (acumulado %s)", len(page), len(items))

        if len(page) < TOP:
            break

        skip += len(page)
        time.sleep(THROTTLE)

    return items


def map_row(t):
    """
    Mapeia um ticket bruto da API pra o formato da tabela
    visualizacao_atual.movidesk_tickets_abertos.
    """
    # Empresa: pego o primeiro client com organization
    clients = t.get("clients") or []
    org = {}
    for c in clients:
        o = (c or {}).get("organization") or {}
        if o:
            org = o
            break

    owner = t.get("owner") or {}

    created_val = norm_ts(t.get("createdDate"))
    last_update_val = norm_ts(t.get("lastUpdate"))

    return {
        "id": iint(t.get("id")),
        "subject": t.get("subject"),
        "type": t.get("type"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        "owner_team": t.get("ownerTeam"),
        # Você falou que aqui deveria ser o serviço de 3º nível
        "service_first_level": t.get("serviceThirdLevel"),
        "created_date": created_val,
        "last_update": last_update_val,
        "contagem": 1,
        "responsavel": owner.get("businessName"),
        "empresa_cod_ref_adicional": org.get("codeReferenceAdditional"),
        "agent_id": iint(owner.get("id")),
        "empresa_id": org.get("id"),
        "empresa_nome": org.get("businessName"),
        "adicional_137641_avaliado_csat": get_cf_value(t, 137641),
        "origin": t.get("origin"),
    }


def ensure_table(conn):
    """
    Cria/ajusta a tabela visualizacao_atual.movidesk_tickets_abertos
    com todos os campos necessários.
    """
    with conn.cursor() as cur:
        cur.execute("create schema if not exists visualizacao_atual")

        cur.execute(
            """
            create table if not exists visualizacao_atual.movidesk_tickets_abertos (
                id                             integer primary key,
                subject                        varchar(350),
                type                           smallint,
                status                         varchar(128),
                base_status                    varchar(128),
                owner_team                     varchar(128),
                service_first_level            varchar(102),
                created_date                   timestamptz,
                last_update                    timestamptz,
                contagem                       integer default 1,
                responsavel                    varchar(255),
                empresa_cod_ref_adicional      varchar(64),
                agent_id                       bigint,
                empresa_id                     text,
                empresa_nome                   text,
                adicional_137641_avaliado_csat text,
                origin                         smallint
            )
            """
        )

        # Garante colunas (idempotente, caso a estrutura já exista)
        cols_alter = [
            ("subject", "varchar(350)"),
            ("type", "smallint"),
            ("status", "varchar(128)"),
            ("base_status", "varchar(128)"),
            ("owner_team", "varchar(128)"),
            ("service_first_level", "varchar(102)"),
            ("created_date", "timestamptz"),
            ("last_update", "timestamptz"),
            ("contagem", "integer default 1"),
            ("responsavel", "varchar(255)"),
            ("empresa_cod_ref_adicional", "varchar(64)"),
            ("agent_id", "bigint"),
            ("empresa_id", "text"),
            ("empresa_nome", "text"),
            ("adicional_137641_avaliado_csat", "text"),
            ("origin", "smallint"),
        ]
        for col, coltype in cols_alter:
            cur.execute(
                f"""
                alter table visualizacao_atual.movidesk_tickets_abertos
                add column if not exists {col} {coltype}
                """
            )

    conn.commit()


def upsert_rows(conn, rows):
    """
    Estratégia simples e rápida:
    - TRUNCATE na tabela
    - INSERT de todos os tickets abertos atuais
    """
    if not rows:
        logger.info("Nenhum ticket aberto retornado da API.")
        with conn.cursor() as cur:
            cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        conn.commit()
        return 0

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
        "contagem",
        "responsavel",
        "empresa_cod_ref_adicional",
        "agent_id",
        "empresa_id",
        "empresa_nome",
        "adicional_137641_avaliado_csat",
        "origin",
    ]

    values = [
        tuple(row.get(c) for c in cols)
        for row in rows
    ]

    sql = f"""
        insert into visualizacao_atual.movidesk_tickets_abertos
        ({", ".join(cols)})
        values %s
    """

    with conn.cursor() as cur:
        cur.execute("truncate table visualizacao_atual.movidesk_tickets_abertos")
        execute_values(cur, sql, values, page_size=200)

    conn.commit()
    logger.info("Inseridos %s tickets na tabela movidesk_tickets_abertos.", len(rows))
    return len(rows)


def main():
    logger.info("Iniciando sync rápido de tickets abertos (index enriquecido).")

    tickets = fetch_open_tickets()
    logger.info("Total de tickets abertos retornados: %s", len(tickets))

    rows = [
        map_row(t)
        for t in tickets
        if isinstance(t, dict) and t.get("id") is not None
    ]

    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        upsert_rows(conn, rows)

    logger.info("Sync de tickets abertos (index) finalizado com sucesso.")


if __name__ == "__main__":
    main()
