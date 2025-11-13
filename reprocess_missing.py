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
    Evita campos que causam 400 no expand.
    """
    params = {
        "$filter": f"id eq {ticket_id}",
        "$select": (
            "id,baseStatus,status,resolvedIn,closedIn,canceledIn,lastUpdate,"
            "origin,category,urgency,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,"
            "owner,ownerTeam,ownerTeamName,organization,customFields"
        ),
        "$expand": "actions($select=id,createdDate,description,public),clients($expand=organization)",
        "$top": 1
    }
    # fallback se ownerTeam* no select causar 400
    try:
        data = od_get("tickets", params)
    except RuntimeError as e:
        if "ownerTeam" in params["$select"]:
            params["$select"] = ",".join(
                p for p in params["$select"].split(",") if not p.lower().startswith("ownerteam")
            )
            data = od_get("tickets", params)
        else:
            raise
    if not data:
        return {}
    return data[0]

def parse_owner(obj):
    oid = None; oname = None
    if isinstance(obj, dict):
        oid = obj.get("id")
        oname = obj.get("businessName") or obj.get("fullName") or obj.get("name")
    return oid, oname

def parse_team(t: Dict[str, Any]):
    team_id = None
    team_name = None
    if isinstance(t.get("ownerTeam"), dict):
        team_id = t["ownerTeam"].get("id") or t["ownerTeam"].get("code")
        team_name = t["ownerTeam"].get("name") or t["ownerTeam"].get("businessName")
    else:
        raw = t.get("ownerTeam") or t.get("ownerTeamId") or t.get("ownerTeamID")
        if raw is not None:
            team_id = str(raw)
        team_name = t.get("ownerTeamName") or team_name
    return team_id, team_name

def map_ticket_to_rows(t: Dict[str, Any]) -> Dict[str, Any]:
    owner_id, owner_name = parse_owner(t.get("owner") or {})
    team_id, team_name   = parse_team(t)

    org_id = None; org_name = None
    org = t.get("organization") or {}
    if org:
        org_id = org.get("id")
        org_name = org.get("name") or org.get("businessName") or org.get("fullName")

    # custom field exemplo
    csat_val = None
    for cf in (t.get("customFields") or []):
        if (cf.get("id") or "").lower() == "adicional_137641_avaliado_csat":
            csat_val = cf.get("value")
            break

    row = {
        "ticket_id": int(t.get("id")),
        "status":    t.get("status") or t.get("baseStatus"),
        "last_resolved_at": t.get("resolvedIn"),
        "last_closed_at":   t.get("closedIn"),
        "last_cancelled_at":t.get("canceledIn"),
        "last_update":      t.get("lastUpdate"),
        "owner_id":   owner_id,
        "owner_name": owner_name,
        "owner_team_id":   team_id,
        "owner_team_name": team_name,
        "organization_id":  org_id,
        "organization_name":org_name,
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
                (ticket_id, status, last_resolved_at, last_closed_at, last_cancelled_at, last_update,
                 origin, category, urgency, service_first_level, service_second_level, service_third_level,
                 owner_id, owner_name, organization_id, organization_name,
                 owner_team_id, owner_team_name, adicional_137641_avaliado_csat)
                values %s
                on conflict (ticket_id) do update set
                  status = excluded.status,
                  last_resolved_at = excluded.last_resolved_at,
                  last_closed_at   = excluded.last_closed_at,
                  last_cancelled_at= excluded.last_cancelled_at,
                  last_update      = excluded.last_update,
                  origin   = excluded.origin,
                  category = excluded.category,
                  urgency  = excluded.urgency,
                  service_first_level  = excluded.service_first_level,
                  service_second_level = excluded.service_second_level,
                  service_third_level  = excluded.service_third_level,
                  owner_id   = excluded.owner_id,
                  owner_name = excluded.owner_name,
                  organization_id  = excluded.organization_id,
                  organization_name= excluded.organization_name,
                  owner_team_id   = excluded.owner_team_id,
                  owner_team_name = excluded.owner_team_name,
                  adicional_137641_avaliado_csat = excluded.adicional_137641_avaliado_csat
            """, [(
                r["ticket_id"], r["status"], r["last_resolved_at"], r["last_closed_at"], r["last_cancelled_at"], r["last_update"],
                r["origin"], r["category"], r["urgency"], r["service_first_level"], r["service_second_level"], r["service_third_level"],
                r["owner_id"], r["owner_name"], r["organization_id"], r["organization_name"],
                r["owner_team_id"], r["owner_team_name"], r["adicional_137641_avaliado_csat"]
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
        # marca atualização simples no sync_control
        for name in names:
            cur.execute("""
                insert into visualizacao_resolvidos.sync_control (name, last_update)
                values (%s, now())
                on conflict (name) do update set last_update = excluded.last_update
            """, (name,))

def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("select id from visualizacao_resolvidos.audit_recent_run order by id desc limit 1")
        row = cur.fetchone()
        if not row:
            logging.info("Nenhum run encontrado em audit_recent_run. Nada a fazer.")
            return
        run_id = row[0]
        logging.info("RUN_ID alvo: %s", run_id)

        cur.execute("""
            select table_name, array_agg(ticket_id order by ticket_id)
              from visualizacao_resolvidos.audit_recent_missing
             where run_id = %s
             group by table_name
        """, (run_id,))
        pend = dict(cur.fetchall() or [])
        logging.info("Pendências por tabela: %s", {k: len(v) for k, v in pend.items()})

    with psycopg2.connect(DSN) as conn:
        # tickets_resolvidos
        if "tickets_resolvidos" in pend:
            ids = pend["tickets_resolvidos"]
            out_rows = []
            for tid in ids:
                try:
                    t = fetch_ticket_with_actions(tid)
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

        # resolvidos_acoes
        if "resolvidos_acoes" in pend:
            ids = pend["resolvidos_acoes"]
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
                    logging.warning("Falha ao buscar ações do ticket %s: %s", tid, e)
            upsert_rows(conn, "resolvidos_acoes", out_rows)
            touch_detail_and_sync(conn, [r["ticket_id"] for r in out_rows], ["resolvidos_acoes"])
            conn.commit()
            logging.info("resolvidos_acoes: upsert %d", len(out_rows))

    logging.info("Fim do reprocessamento.")

if __name__ == "__main__":
    main()
