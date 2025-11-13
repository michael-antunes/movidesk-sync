# Troque seu sync_resolved_detail.py por este trecho ajustado:

# ... (cabeçalho e imports iguais)

# --------- dentro de fetch_pages() ---------
select_fields = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency",
    "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
    "ownerTeam"                 # <---- ADICIONADO
])
expand = "owner,clients($expand=organization)"  # owner traz o id/nome do responsável
# -------------------------------------------

# --------- dentro de _fetch_group_by_ids() e fetch_by_ids() ---------
select_fields = ",".join([
    "id","status","resolvedIn","closedIn","canceledIn","lastUpdate",
    "origin","category","urgency",
    "serviceFirstLevel","serviceSecondLevel","serviceThirdLevel",
    "ownerTeam"                 # <---- ADICIONADO
])
expand = "owner,clients($expand=organization)"
# --------------------------------------------------------------------

def map_row(t: Dict[str, Any]) -> Dict[str, Any]:
    owner = t.get("owner") or {}
    org_id = None
    org_name = None
    clients = t.get("clients") or []
    if clients:
        org = clients[0].get("organization") or {}
        org_id = org.get("id")
        org_name = org.get("businessName") or org.get("fullName") or org.get("name")

    return {
        "ticket_id": t.get("id"),
        "status": t.get("status"),
        "last_resolved_at": to_utc(t.get("resolvedIn")),
        "last_closed_at": to_utc(t.get("closedIn")),
        "last_cancelled_at": to_utc(t.get("canceledIn")),
        "last_update": to_utc(t.get("lastUpdate")),
        "origin": t.get("origin"),
        "category": t.get("category"),
        "urgency": t.get("urgency"),
        "service_first_level": t.get("serviceFirstLevel"),
        "service_second_level": t.get("serviceSecondLevel"),
        "service_third_level": t.get("serviceThirdLevel"),
        "owner_id": owner.get("id"),                                  # <---- ADICIONADO
        "owner_name": owner.get("businessName") or owner.get("fullName") or owner.get("name"),
        "owner_team_name": t.get("ownerTeam"),                        # <---- ADICIONADO (string)
        "organization_id": org_id,
        "organization_name": org_name,
    }

UPSERT_SQL = """
insert into visualizacao_resolvidos.tickets_resolvidos
(ticket_id,status,last_resolved_at,last_closed_at,last_cancelled_at,last_update,
 origin,category,urgency,service_first_level,service_second_level,service_third_level,
 owner_id,owner_name,owner_team_name,organization_id,organization_name)          -- <---- colunas incluídas
values
(%(ticket_id)s,%(status)s,%(last_resolved_at)s,%(last_closed_at)s,%(last_cancelled_at)s,%(last_update)s,
 %(origin)s,%(category)s,%(urgency)s,%(service_first_level)s,%(service_second_level)s,%(service_third_level)s,
 %(owner_id)s,%(owner_name)s,%(owner_team_name)s,%(organization_id)s,%(organization_name)s)
on conflict (ticket_id) do update set
 status               = excluded.status,
 last_resolved_at     = excluded.last_resolved_at,
 last_closed_at       = excluded.last_closed_at,
 last_cancelled_at    = excluded.last_cancelled_at,
 last_update          = excluded.last_update,
 origin               = excluded.origin,
 category             = excluded.category,
 urgency              = excluded.urgency,
 service_first_level  = excluded.service_first_level,
 service_second_level = excluded.service_second_level,
 service_third_level  = excluded.service_third_level,
 owner_id             = excluded.owner_id,                             -- <----
 owner_name           = excluded.owner_name,
 owner_team_name      = excluded.owner_team_name,                      -- <----
 organization_id      = excluded.organization_id,
 organization_name    = excluded.organization_name
"""
