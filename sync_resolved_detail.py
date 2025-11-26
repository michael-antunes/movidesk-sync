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

# Campo adicional "Nome" – código 29077
# se não tiver definido no secret, força como 29077
CUSTOM_NOME_FIELD_ID = os.getenv("MOVIDESK_CUSTOM_NOME_FIELD_ID", "29077").strip()
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


def _normalize_custom_value(val):
    if isinstance(val, list):
        val = ", ".join(str(x) for x in val if x not in ("", None))
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
    return val


def extract_custom_nome(ticket):
    """
    Prioridade:
    1) Campo customizado com ID = CUSTOM_NOME_FIELD_ID (ex.: 29077)
    2) Campo cujo label contenha 'nome'
    3) Fallback: tenta pegar algum nome do cliente (quando fizer sentido)
    """
    values = ticket.get("customFieldValues") or []
    if not isinstance(values, list):
        values = []

    # 1) Tenta primeiro pelo ID configurado (29077)
    if CUSTOM_NOME_FIELD_ID:
        for e in values:
            if not isinstance(e, dict):
                continue
            cid = (
                e.get("customFieldId")
                or e.get("id")
                or e.get("customField")
                or e.get("customFieldID")
            )
            cid = str(cid).strip() if cid is not None else ""
            if cid == CUSTOM_NOME_FIELD_ID:
                val = (
                    e.get("value")
                    or e.get("currentValue")
                    or e.get("text")
                    or e.get("valueName")
                    or e.get("name")
                )
                val = _normalize_custom_value(val)
                if val is not None:
                    return val

    # 2) Tenta por label / nome de campo contendo "nome"
    if CUSTOM_NOME_FIELD_LABEL:
        target_label = CUSTOM_NOME_FIELD_LABEL
    else:
        # se não veio nada via env, usamos "nome" como padrão
        target_label = "nome"

    for e in values:
        if not isinstance(e, dict):
            continue
        label = (
            e.get("field")
            or e.get("name")
            or e.get("label")
            or e.get("customField")
            or e.get("customFieldLabel")
        )
        if not isinstance(label, str):
            continue
        label_l = label.strip().lower()
        if not label_l:
            continue

        # se o label bater exatamente com o configurado OU contiver 'nome'
        if label_l == target_label or ("nome" in label_l and target_label in ("", "nome")):
            val = (
                e.get("value")
                or e.get("currentValue")
                or e.get("text")
                or e.get("valueName")
                or e.get("name")
            )
            val = _normalize_custom_value(val)
            if val is not None:
                return val

    # 3) Fallback: tenta achar algum "nome" do cliente (melhor do que NULL)
    clients = ticket.get("clients") or []
    c0 = clients[0] if isinstance(clients, list) and clients else {}
    if isinstance(c0, dict):
        # tenta alguns campos comuns de pessoa/contato do Movidesk
        for key in (
            "personName",
            "personFullName",
            "businessName",
            "name",
            "contactName",
        ):
            val = c0.get(key)
            val = _normalize_custom_value(val)
            if val is not None:
                return val

    return None


def map_row(t):
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

    null_keys = [k for k, v in row.items() if v is None and k != "owner_team_name"]
    if null_keys:
        print(
            f"[DEBUG] ticket {row['ticket_id']} campos ainda NULL depois do map_row: {null_keys}"
        )
    return row


UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,owner_id,owner_name,owner_team_name,origin,
 organization_id,organization_name,category,urgency,
 service_first_level,service_second_level,service_third_level,
 last_update,subject,adicional_nome)
values
(%(ticket_id)s,%(status)s,%(owner_id)s,%(owner_name)s,%(owner_team_name)s,%(origin)s,
 %(organization_id)s,%(organization_name)s,%(category)s,%(urgency)s,
 %(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(last_update)s,%(subject)s,%(adicional_nome)s)
on conflict (ticket_id) do update set
    status = excluded.status,
    owner_id = excluded.owner_id,
    owner_name = excluded.owner_name,
    owner_team_name = excluded.owner_team_name,
    origin = excluded.origin,
    organization_id = excluded.organization_id,
    organization_name = excluded.organization_name,
    category = excluded.category,
    urgency = excluded.urgency,
    service_first_level = excluded.service_first_level,
    service_second_level = excluded.service_second_level,
    service_third_level = excluded.service_third_level,
    last_update = excluded.last_update,
    subject = excluded.subject,
    adicional_nome = excluded.adicional_nome
"""


def select_missing_ticket_ids(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select ticket_id
            from visualizacao_resolvidos.audit_recent_missing
            where table_name in ('tickets_resolvidos','visualizacao_resolvidos.tickets_resolvidos')
            order by run_id desc, ticket_id desc
            limit %s
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
        for tid in ticket_ids:
            try:
                t = fetch_ticket_detail(tid)
            except requests.HTTPError as e:
                print(f"Erro HTTP fatal para ticket {tid}: {e}")
                # se der pau de HTTP, deixa o ticket na missing para outra rodada
                continue

            if not t:
                print(
                    f"[WARN] ticket {tid}: API não retornou dados, mantendo na missing."
                )
                continue

            if isinstance(t, list):
                t = t[0] if t else None

            if not isinstance(t, dict) or t.get("id") is None:
                print(
                    f"[WARN] ticket {tid}: resposta inesperada da API: {t}, mantendo na missing."
                )
                continue

            row = map_row(t)
            if row.get("ticket_id") is None:
                print(
                    f"[WARN] ticket {tid}: map_row sem ticket_id, mantendo na missing."
                )
                continue

            rows.append(row)
            time.sleep(THROTTLE)

        n_upsert = upsert_rows(conn, rows)
        print(f"UPSERT detail: {n_upsert} linhas atualizadas.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
