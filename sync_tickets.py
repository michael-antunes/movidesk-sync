import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def _norm(s):
    if s is None:
        return None
    return str(s).strip().lower()

def load_empresas_name_to_coderef(conn):
    sql = """
    SELECT lower(trim(businessname)) AS k, codereferenceadditional
    FROM visualizacao_empresa.empresas
    WHERE businessname IS NOT NULL AND codereferenceadditional IS NOT NULL
    """
    m = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for k, coderef in cur.fetchall():
            if k and coderef:
                m[k] = str(coderef)
    return m

def fetch_tickets():
    if not API_TOKEN:
        raise RuntimeError("MOVIDESK_TOKEN vazio")
    url = "https://api.movidesk.com/public/v1/tickets"

    out = []
    skip = 0
    top = 500

    while True:
        params = {
            "token": API_TOKEN,
            "$select": "id,protocol,type,subject,status,baseStatus,ownerTeam,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,createdDate,lastUpdate",
            # ponto-e-vírgula entre $select e $expand da subcoleção (sintaxe oficial)
            "$expand": (
                "owner($select=id,businessName),"
                "clients($select=id,businessName,personType,profileType;"
                "$expand=organization($select=id,businessName,codeReferenceAdditional)),"
                "createdBy($select=id,businessName)"
            ),
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
            owner = t.get("owner") or {}
            responsavel = owner.get("businessName")
            try:
                agent_id = int(owner.get("id")) if owner.get("id") is not None else None
            except Exception:
                agent_id = None

            # escolha da organização do ticket a partir dos clients
            empresa_nome = None
            empresa_coderef = None
            for c in (t.get("clients") or []):
                org = (c or {}).get("organization") or {}
                oname = org.get("businessName")
                ocoderef = org.get("codeReferenceAdditional")
                if oname and empresa_nome is None:
                    empresa_nome = oname
                if ocoderef:
                    empresa_coderef = str(ocoderef)
                    break  # já temos a chave única

            out.append({
                "id": t["id"],
                "protocol": t.get("protocol"),
                "type": t.get("type"),
                "subject": t.get("subject"),
                "status": t.get("status"),
                "base_status": t.get("baseStatus"),
                "owner_team": t.get("ownerTeam"),
                "service_first_level": t.get("serviceFirstLevel"),
                "service_second_level": t.get("serviceSecondLevel"),
                "service_third_level": t.get("serviceThirdLevel"),
                "created_date": t.get("createdDate"),
                "last_update": t.get("lastUpdate"),
                "responsavel": responsavel,
                "agent_id": agent_id,
                "empresa_nome": empresa_nome,
                "empresa_coderef": empresa_coderef
            })

        if len(batch) < top:
            break
        skip += len(batch)

    return out

def upsert_tickets(conn, rows):
    if not rows:
        return
    sql = """
INSERT INTO visualizacao_atual.movidesk_tickets_abertos
  (id, protocol, subject, type, status, base_status, owner_team,
   service_first_level, service_second_level, service_third_level,
   created_date, last_update, responsavel, agent_id,
   empresa_cod_ref_adicional, empresa_nome)
VALUES
  (%(id)s, %(protocol)s, %(subject)s, %(type)s, %(status)s, %(base_status)s, %(owner_team)s,
   %(service_first_level)s, %(service_second_level)s, %(service_third_level)s,
   %(created_date)s, %(last_update)s, %(responsavel)s, %(agent_id)s,
   %(empresa_coderef)s, %(empresa_nome)s)
ON CONFLICT (id) DO UPDATE SET
  protocol = EXCLUDED.protocol,
  subject = EXCLUDED.subject,
  type = EXCLUDED.type,
  status = EXCLUDED.status,
  base_status = EXCLUDED.base_status,
  owner_team = EXCLUDED.owner_team,
  service_first_level = EXCLUDED.service_first_level,
  service_second_level = EXCLUDED.service_second_level,
  service_third_level = EXCLUDED.service_third_level,
  created_date = EXCLUDED.created_date,
  last_update = EXCLUDED.last_update,
  responsavel = EXCLUDED.responsavel,
  agent_id = EXCLUDED.agent_id,
  empresa_cod_ref_adicional = EXCLUDED.empresa_cod_ref_adicional,
  empresa_nome = EXCLUDED.empresa_nome;
"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=300)
    conn.commit()

def cleanup_resolvidos(conn):
    with conn.cursor() as cur:
        cur.execute("""
DELETE FROM visualizacao_atual.movidesk_tickets_abertos t
USING visualizacao_resolvidos.tickets_resolvidos r
WHERE r.ticket_id = t.id
""")
    conn.commit()

def backfill_coderef_by_name(conn):
    sql = """
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE t.empresa_cod_ref_adicional IS NULL
  AND e.businessname IS NOT NULL
  AND lower(trim(e.businessname)) = lower(trim(t.empresa_nome));
"""
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    conn = psycopg2.connect(DSN)
    rows = fetch_tickets()
    upsert_tickets(conn, rows)
    cleanup_resolvidos(conn)
    backfill_coderef_by_name(conn)
    conn.close()

if __name__ == "__main__":
    main()
