# -*- coding: utf-8 -*-
import os, time, logging, json
from typing import Dict, Any, List
from urllib.parse import urlencode
import requests, psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ["MOVIDESK_TOKEN"]
DSN   = os.environ["NEON_DSN"]
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))

def od_get(path: str, params: dict):
    params = {"token": TOKEN, **params}
    url = f"{BASE}/{path}?{urlencode(params, safe='(),$= ')}"
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    return r.json()

def fetch_ticket_with_actions(ticket_id: int) -> Dict[str, Any]:
    """
    Busca 1 ticket + suas ações.
    Observação importante: NÃO incluir baseStatus no $select do expand de actions (gera 400).
    """
    params = {
        "$filter": f"id eq {ticket_id}",
        # no select do ticket você pode ajustar os campos conforme a necessidade;
        # deixei genérico para não falhar caso algum não exista
        "$select": "id,baseStatus,status,resolvedIn,closedIn,origin,category,urgency,"
                   "serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,"
                   "owner,organization,customFields",
        "$expand": "actions($select=id,createdDate,description,public)",
        "$take": 1
    }
    data = od_get("tickets", params)
    if not data:
        return {}
    return data[0]

def map_ticket_to_rows(t: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o ticket do Movidesk para o schema tickets_resolvidos.
    """
    owner = t.get("owner") or {}
    org   = t.get("organization") or {}

    # tenta puxar o custom field "adicional_137641_avaliado_csat" se existir
    csat_val = None
    for cf in t.get("customFields", []) or []:
        if (cf.get("id") or "").lower() == "adicional_137641_avaliado_csat":
            csat_val = cf.get("value")
            break

    row = {
        "ticket_id": int(t.get("id")),
        "status":    t.get("status") or t.get("baseStatus"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at":   t.get("closedIn"),
        "responsible_id":   owner.get("id"),
        "responsible_name": owner.get("name"),
        "organization_id":  org.get("id"),
        "organization_name":org.get("name"),
        "origin":  t.get("origin"),
        "category":t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level":  t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level":  t.get("serviceThirdLevel"),
        "adicional_137641_avaliado_csat": csat_val
    }
    return row

def map_actions_json(t: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Monta o JSON de ações para gravar em resolvidos_acoes.acoes (jsonb).
    Inclui 'public' (bool) e um 'type' auxiliar (2=public,1=interno) para
    as colunas geradas que contam descrições.
    """
    acoes = []
    for a in t.get("actions") or []:
        is_pub = bool(a.get("public"))
        acoes.append({
            "id": a.get("id"),
            "createdDate": a.get("createdDate"),
            "description": a.get("description"),
            "public": is_pub,
            "type": 2 if is_pub else 1
        })
    return acoes

def upsert_rows(conn, table: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    with conn.cursor() as cur:
        if table == "tickets_resolvidos":
            execute_values(cur, """
                insert into visualizacao_resolvidos.tickets_resolvidos
                (ticket_id, status, last_resolved_at, last_closed_at,
                 responsible_id, responsible_name, organization_id, organization_name,
                 origin, category, urgency, service_first_level, service_second_level,
                 service_third_level, adicional_137641_avaliado_csat)
                values %s
                on conflict (ticket_id) do update set
                  status = excluded.status,
                  last_resolved_at = excluded.last_resolved_at,
                  last_closed_at   = excluded.last_closed_at,
                  responsible_id   = excluded.responsible_id,
                  responsible_name = excluded.responsible_name,
                  organization_id  = excluded.organization_id,
                  organization_name= excluded.organization_name,
                  origin   = excluded.origin,
                  category = excluded.category,
                  urgency  = excluded.urgency,
                  service_first_level  = excluded.service_first_level,
                  service_second_level = excluded.service_second_level,
                  service_third_level  = excluded.service_third_level,
                  adicional_137641_avaliado_csat = excluded.adicional_137641_avaliado_csat
            """, [(
                r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"],
                r["responsible_id"], r["responsible_name"], r["organization_id"], r["organization_name"],
                r["origin"], r["category"], r["urgency"], r["service_first_level"], r["service_second_level"],
                r["service_third_level"], r["adicional_137641_avaliado_csat"]
            ) for r in rows])

        elif table == "resolvidos_acoes":
            execute_values(cur, """
                insert into visualizacao_resolvidos.resolvidos_acoes
                    (ticket_id, acoes)
                values %s
                on conflict (ticket_id) do update set
                    acoes = excluded.acoes
            """, [ (r["ticket_id"], json.dumps(r["acoes"])) for r in rows ])

def touch_detail_and_sync(conn, ticket_ids: List[int], names: List[str]):
    if not ticket_ids:
        return
    with conn.cursor() as cur:
        # detail_control
        execute_values(cur, """
            insert into visualizacao_resolvidos.detail_control (ticket_id, last_update)
            values %s
            on conflict (ticket_id) do update set last_update = excluded.last_update
        """, [(tid, None) for tid in ticket_ids])  # None => DEFAULT now() não existe; então usamos now() direto:
        cur.execute("""
            update visualizacao_resolvidos.detail_control
               set last_update = now()
             where ticket_id = any(%s)
        """, (ticket_ids,))

        # sync_control para as tabelas processadas
        for name in names:
            cur.execute("""
                insert into visualizacao_resolvidos.sync_control (name, last_update)
                values (%s, now())
                on conflict (name) do update set last_update = excluded.last_update
            """, (name,))

def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        # pega o último run
        cur.execute("select id from visualizacao_resolvidos.audit_recent_run order by id desc limit 1")
        row = cur.fetchone()
        if not row:
            logging.info("Nenhum run encontrado em audit_recent_run. Nada a fazer.")
            return
        run_id = row[0]
        logging.info("RUN_ID alvo: %s", run_id)

        # pendências por tabela
        cur.execute("""
            select table_name, array_agg(ticket_id order by ticket_id)
              from visualizacao_resolvidos.audit_recent_missing
             where run_id = %s
             group by table_name
        """, (run_id,))
        pend = dict(cur.fetchall() or [])
        logging.info("Pendências por tabela: %s", {k: len(v) for k, v in pend.items()})

    # processa uma vez e reaproveita conexão (menos latência)
    with psycopg2.connect(DSN) as conn:
        # --------- tickets_resolvidos ----------
        if "tickets_resolvidos" in pend:
            ids = pend["tickets_resolvidos"]
            logging.info("Buscando %d tickets (detalhes)...", len(ids))
            out_rows = []
            for tid in ids:
                try:
                    t = fetch_ticket_with_actions(tid)  # vem com ações também mas ignoramos aqui
                    if not t:
                        continue
                    out_rows.append(map_ticket_to_rows(t))
                    time.sleep(THROTTLE)
                except Exception as e:
                    logging.warning("Falha ao buscar ticket %s: %s", tid, e)
            upsert_rows(conn, "tickets_resolvidos", out_rows)
            touch_detail_and_sync(conn, [r["ticket_id"] for r in out_rows], ["tickets_resolvidos"])
            conn.commit()
            logging.info("tickets_resolvidos: upsert %d", len(out_rows))

        # --------- resolvidos_acoes ----------
        if "resolvidos_acoes" in pend:
            ids = pend["resolvidos_acoes"]
            logging.info("Buscando ações de %d tickets (1 a 1 com $expand=actions)...", len(ids))
            out_rows = []
            for tid in ids:
                try:
                    t = fetch_ticket_with_actions(tid)
                    if not t:
                        continue
                    acoes = map_actions_json(t)
                    out_rows.append({"ticket_id": int(tid), "acoes": acoes})
                    time.sleep(THROTTLE)
                except Exception as e:
                    # >>> AQUI estava o 400 do baseStatus em actions; removido do expand.
                    logging.warning("Falha ao buscar ações do ticket %s: %s", tid, e)
            upsert_rows(conn, "resolvidos_acoes", out_rows)
            touch_detail_and_sync(conn, [r["ticket_id"] for r in out_rows], ["resolvidos_acoes"])
            conn.commit()
            logging.info("resolvidos_acoes: upsert %d", len(out_rows))

    logging.info("Fim do reprocessamento.")

if __name__ == "__main__":
    main()
