import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def fetch_solicitantes():
    url = "https://api.movidesk.com/public/v1/tickets"
    out = []
    skip = 0
    top = 1000
    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id",
            "$expand": "clients($select=id,businessName,emails,profileType),createdBy($select=id,businessName,emails,profileType)",
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        for t in batch:
            sel = None
            for c in t.get("clients") or []:
                if c.get("businessName"):
                    sel = c
                    break
            if not sel:
                sel = t.get("createdBy") or {}
            nome = sel.get("businessName")
            if not nome:
                continue
            emails = sel.get("emails") or []
            email = emails[0] if isinstance(emails, list) and emails else None
            tipo = sel.get("profileType")
            try:
                tipo = int(tipo) if tipo is not None else None
            except Exception:
                tipo = None
            out.append({
                "ticket_id": t["id"],
                "sid": sel.get("id"),
                "nome": nome,
                "email": email,
                "tipo": tipo
            })
        if len(batch) < top:
            break
        skip += len(batch)
    return out

def update_solicitante(conn, rows):
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
    update_solicitante(conn, rows)
    backfill_cod_ref(conn)
    conn.close()

if __name__ == "__main__":
    main()
