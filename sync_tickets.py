import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def fetch_solicitantes():
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN vazio")
    url = "https://api.movidesk.com/public/v1/tickets"
    out = []
    skip = 0
    top = 500
    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id",
            "$expand": "clients,createdBy",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break
        for t in batch:
            sel = None
            cls = t.get("clients") or []
            for c in cls:
                if isinstance(c, dict) and c.get("businessName"):
                    sel = c
                    break
            if not sel:
                sel = t.get("createdBy") or {}
            nome = sel.get("businessName")
            if not nome:
                continue
            emails = sel.get("emails") or []
            email = emails[0] if isinstance(emails, list) and emails else None
            out.append({
                "ticket_id": t["id"],
                "sid": sel.get("id"),
                "nome": nome,
                "email": email,
                "tipo": sel.get("profileType") if isinstance(sel.get("profileType"), int) else None
            })
        if len(batch) < top:
            break
        skip += len(batch)
    return out

def update_db(conn, rows):
    if not rows:
        return
    sql = """
    UPDATE visualizacao_atual.movidesk_tickets_abertos
    SET solicitante = %(nome)s,
        solicitante_nome = %(nome)s,
        solicitante_id = %(sid)s,
        solicitante_email = %(email)s,
        solicitante_tipo = %(tipo)s
    WHERE id = %(ticket_id)s;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()

def backfill_cod_ref(conn):
    sql = """
    UPDATE visualizacao_atual.movidesk_tickets_abertos t
    SET empresa_cod_ref_adicional = e.codereferenceadditional,
        solicitante_code_ref_adicional = e.codereferenceadditional
    FROM visualizacao_empresa.empresas e
    WHERE e.businessname IS NOT NULL
      AND lower(trim(e.businessname)) = lower(trim(coalesce(t.solicitante_nome, t.solicitante)))
      AND (t.empresa_cod_ref_adicional IS DISTINCT FROM e.codereferenceadditional
           OR t.solicitante_code_ref_adicional IS DISTINCT FROM e.codereferenceadditional);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    rows = fetch_solicitantes()
    conn = psycopg2.connect(DSN)
    update_db(conn, rows)
    backfill_cod_ref(conn)
    conn.close()

if __name__ == "__main__":
    main()
