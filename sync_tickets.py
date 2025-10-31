import os
import requests
import psycopg2
import psycopg2.extras

API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN = os.getenv("NEON_DSN", "").strip()

def _to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def _ptype(obj):
    for k in ("profileType", "personType"):
        if k in obj and obj[k] is not None:
            try:
                return int(obj[k])
            except Exception:
                try:
                    return int(str(obj[k]).strip())
                except Exception:
                    return None
    return None

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
            "$expand": (
                "owner($select=id,businessName),"
                "clients($select=id,businessName,profileType,personType),"
                "createdBy($select=id,businessName,profileType,personType)"
            ),
            "$filter": "(status eq 'Em atendimento' or status eq 'Aguardando' or status eq 'Novo')",
            "$top": top,
            "$skip": skip
        }
        r = requests.get(url, params=params, timeout=90)
        if r.status_code >= 400:
            raise RuntimeError(f"Tickets HTTP {r.status_code}: {r.text}")
        batch = r.json()
        if not batch:
            break

        for t in batch:
            owner = t.get("owner") or {}
            responsavel = owner.get("businessName")
            agent_id = _to_int(owner.get("id"))

            empresa_id = None
            empresa_nome = None

            for c in t.get("clients") or []:
                pt = _ptype(c or {})
                if pt in (2, 3):
                    empresa_id = str(c.get("id")) if c.get("id") is not None else None
                    empresa_nome = c.get("businessName")
                    break

            if not empresa_id:
                cb = t.get("createdBy") or {}
                if _ptype(cb) in (2, 3):
                    empresa_id = str(cb.get("id")) if cb.get("id") is not None else None
                    empresa_nome = cb.get("businessName")

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
                "empresa_id": empresa_id,
                "empresa_nome": empresa_nome
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
   empresa_id, empresa_nome)
VALUES
  (%(id)s, %(protocol)s, %(subject)s, %(type)s, %(status)s, %(base_status)s, %(owner_team)s,
   %(service_first_level)s, %(service_second_level)s, %(service_third_level)s,
   %(created_date)s, %(last_update)s, %(responsavel)s, %(agent_id)s,
   %(empresa_id)s, %(empresa_nome)s)
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
  empresa_id = EXCLUDED.empresa_id,
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

def backfill_codref(conn):
    with conn.cursor() as cur:
        cur.execute("""
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE e.id::text = t.empresa_id
  AND t.empresa_id IS NOT NULL
  AND t.empresa_cod_ref_adicional IS DISTINCT FROM e.codereferenceadditional;
        """)
        cur.execute("""
UPDATE visualizacao_atual.movidesk_tickets_abertos t
SET empresa_cod_ref_adicional = e.codereferenceadditional
FROM visualizacao_empresa.empresas e
WHERE t.empresa_cod_ref_adicional IS NULL
  AND t.empresa_nome IS NOT NULL
  AND e.businessname IS NOT NULL
  AND lower(trim(e.businessname)) = lower(trim(t.empresa_nome));
        """)
    conn.commit()

def main():
    rows = fetch_tickets()
    conn = psycopg2.connect(DSN)
    upsert_tickets(conn, rows)
    cleanup_resolvidos(conn)
    backfill_codref(conn)
    conn.close()

if __name__ == "__main__":
    main()
