import os
import time
import requests
import psycopg2
import psycopg2.extras


API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.3"))
BATCH_LIMIT = int(os.getenv("MOVIDESK_DETAIL_BATCH_LIMIT", "100"))

# Campo adicional usado para "adicional_nome"
CUSTOM_NOME_FIELD_ID = os.getenv("MOVIDESK_CUSTOM_NOME_FIELD_ID", "").strip()
CUSTOM_NOME_FIELD_LABEL = os.getenv("MOVIDESK_CUSTOM_NOME_FIELD_LABEL", "").strip().lower()


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


def http_get(url, params=None, timeout=90, allow_400=False):
    while True:
        r = requests.get(url, params=params, timeout=timeout)

        # throttling da API Movidesk
        if r.status_code in (429, 503):
            retry = r.headers.get("retry-after")
            wait = int(retry) if str(retry).isdigit() else 60
            print(f"Throttling {r.status_code}, esperando {wait}s")
            time.sleep(wait)
            continue

        if allow_400 and r.status_code == 400:
            print("HTTP 400 para", url, "params", params)
            return None

        if r.status_code == 404:
            print("HTTP 404 para", url, "params", params)
            return None

        if r.status_code >= 400:
            try:
                print("HTTP ERROR", r.status_code, r.text[:800])
            except Exception:
                pass
            r.raise_for_status()

        return r.json() if r.text else None


def fetch_ticket_detail(ticket_id):
    """
    Busca o ticket completo na API.
    """
    url = f"{API_BASE}/tickets"
    params = {
        "token": API_TOKEN,
        "id": ticket_id,
        "includeDeletedItems": "true",
        "$expand": "clients($expand=organization),owner,customFieldValues",
    }

    data = http_get(url, params=params, timeout=120, allow_400=True)

    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict) and data.get("id") is not None:
        return data
    return None


def extract_custom_nome(ticket):
    """
    Extrai o valor do campo adicional configurado para 'adicional_nome'.
    Pode usar o ID do campo ou o label, conforme variáveis de ambiente.
    """
    values = ticket.get("customFieldValues") or []
    if not isinstance(values, list):
        return None

    # Prioriza buscar pelo ID exato
    if CUSTOM_NOME_FIELD_ID:
        for e in values:
            if not isinstance(e, dict):
                continue
            cid = str(e.get("customFieldId") or e.get("id") or "").strip()
            if cid == CUSTOM_NOME_FIELD_ID:
                val = (
                    e.get("value")
                    or e.get("currentValue")
                    or e.get("text")
                    or e.get("valueName")
                    or e.get("name")
                )
                if isinstance(val, list):
                    val = ", ".join(str(x) for x in val if x not in ("", None))
                if isinstance(val, str) and val.strip():
                    return val.strip()

    # Fallback por label / nome do campo
    if CUSTOM_NOME_FIELD_LABEL:
        for e in values:
            if not isinstance(e, dict):
                continue
            label = (
                str(
                    e.get("field")
                    or e.get("name")
                    or e.get("label")
                    or ""
                )
                .strip()
                .lower()
            )
            if label == CUSTOM_NOME_FIELD_LABEL:
                val = (
                    e.get("value")
                    or e.get("currentValue")
                    or e.get("text")
                    or e.get("valueName")
                    or e.get("name")
                )
                if isinstance(val, list):
                    val = ", ".join(str(x) for x in val if x not in ("", None))
                if isinstance(val, str) and val.strip():
                    return val.strip()

    return None


def map_row(t):
    """
    Mapeia o JSON do ticket da API para as colunas de tickets_resolvidos.
    Os campos de datas de resolução/fechamento/cancelamento e CSAT
    são tratados pelas triggers no banco, com base em resolucao_por_status,
    FCR/CSAT etc. Aqui focamos em status, dono, organização, serviços etc.
    """
    owner = t.get("owner") or {}
    clients = t.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    org = c0.get("organization") or {}

    row = {
        "ticket_id": iint(t.get("id")),
        "status": t.get("status"),
        "owner_id": iint(owner.get("id") or owner.get("personId")),
        "owner_name": owner.get("businessName") or owner.get("name"),
        "owner_team_name": t.get("ownerTeam"),
        "origin": t.get("origin"),
        "organization_id": org.get("id"),
        "organization_name": org.get("businessName"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "last_update": norm_ts(t.get("lastUpdate")),
        "subject": t.get("subject"),
        "adicional_nome": extract_custom_nome(t),
    }

    # Só loga para debug; quem decide manter em missing ou não é o banco.
    null_keys = [k for k, v in row.items() if v is None and k != "owner_team_name"]
    if null_keys:
        print(
            f"[DEBUG] ticket {row['ticket_id']} campos ainda NULL depois do map_row: {null_keys}"
        )

    return row


UPSERT_SQL = """
    INSERT INTO visualizacao_resolvidos.tickets_resolvidos (
        ticket_id,
        status,
        owner_id,
        owner_name,
        owner_team_name,
        origin,
        organization_id,
        organization_name,
        category,
        urgency,
        service_first_level,
        service_second_level,
        service_third_level,
        last_update,
        subject,
        adicional_nome
    )
    VALUES (
        %(ticket_id)s,
        %(status)s,
        %(owner_id)s,
        %(owner_name)s,
        %(owner_team_name)s,
        %(origin)s,
        %(organization_id)s,
        %(organization_name)s,
        %(category)s,
        %(urgency)s,
        %(service_first_level)s,
        %(service_second_level)s,
        %(service_third_level)s,
        %(last_update)s,
        %(subject)s,
        %(adicional_nome)s
    )
    ON CONFLICT (ticket_id) DO UPDATE SET
        status              = EXCLUDED.status,
        owner_id            = EXCLUDED.owner_id,
        owner_name          = EXCLUDED.owner_name,
        owner_team_name     = EXCLUDED.owner_team_name,
        origin              = EXCLUDED.origin,
        organization_id     = EXCLUDED.organization_id,
        organization_name   = EXCLUDED.organization_name,
        category            = EXCLUDED.category,
        urgency             = EXCLUDED.urgency,
        service_first_level = EXCLUDED.service_first_level,
        service_second_level= EXCLUDED.service_second_level,
        service_third_level = EXCLUDED.service_third_level,
        last_update         = EXCLUDED.last_update,
        subject             = EXCLUDED.subject,
        adicional_nome      = EXCLUDED.adicional_nome
"""


def select_missing_ticket_ids(conn, limit):
    """
    Busca os tickets na fila de auditoria que são da tabela tickets_resolvidos.
    As triggers no banco (resolvidos_acoes / tickets_resolvidos) garantem
    que tickets com problemas de datas ou CSAT também sejam enfileirados aqui.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticket_id
              FROM visualizacao_resolvidos.audit_recent_missing
             WHERE table_name IN (
                     'tickets_resolvidos',
                     'visualizacao_resolvidos.tickets_resolvidos'
                   )
             ORDER BY run_id DESC, ticket_id DESC
             LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


def upsert_rows(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


def main():
    if not API_TOKEN or not NEON_DSN:
        raise RuntimeError("Defina MOVIDESK_TOKEN e NEON_DSN nos secrets.")

    conn = psycopg2.connect(NEON_DSN, sslmode="require")

    try:
        ticket_ids = select_missing_ticket_ids(conn, BATCH_LIMIT)
        if not ticket_ids:
            print(
                "Nenhum ticket pendente em audit_recent_missing para tickets_resolvidos."
            )
            return

        print(
            f"Reprocessando {len(ticket_ids)} tickets da audit_recent_missing (mais novos primeiro): {ticket_ids}"
        )

        rows = []
        kept_in_missing = []

        for tid in ticket_ids:
            try:
                t = fetch_ticket_detail(tid)
            except requests.HTTPError as e:
                # Erro 4xx/5xx que não dá para tratar aqui: mantém na missing.
                print(f"Erro HTTP fatal para ticket {tid}: {e}")
                kept_in_missing.append(tid)
                continue

            if not t:
                print(
                    f"[WARN] ticket {tid}: API não retornou dados, mantendo na missing."
                )
                kept_in_missing.append(tid)
                continue

            if isinstance(t, list):
                t = t[0] if t else None

            if not isinstance(t, dict) or t.get("id") is None:
                print(
                    f"[WARN] ticket {tid}: resposta inesperada da API: {t}, mantendo na missing."
                )
                kept_in_missing.append(tid)
                continue

            row = map_row(t)

            if row.get("ticket_id") is None:
                print(
                    f"[WARN] ticket {tid}: map_row sem ticket_id, mantendo na missing."
                )
                kept_in_missing.append(tid)
                continue

            rows.append(row)
            # NÃO removemos da audit_recent_missing aqui.
            # As triggers (fn_clear_audit_on_ticket_upsert / regras de watch)
            # é que vão decidir se o ticket deve sair da fila ou permanecer.
            time.sleep(THROTTLE)

        n_upsert = upsert_rows(conn, rows)
        print(f"UPSERT detail: {n_upsert} linhas atualizadas.")

        if kept_in_missing:
            print(
                "Tickets que permaneceram em audit_recent_missing por falha/mapeamento:",
                kept_in_missing,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
