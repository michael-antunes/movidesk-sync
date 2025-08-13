import os
import time
import requests
import psycopg2
from psycopg2.extras import Json

TOKEN = os.environ.get("MOVIDESK_TOKEN", "")
DSN = os.environ.get("NEON_DSN", "")
BASE = "https://api.movidesk.com/public/v1"

# Campo adicional de Pessoa: "Time (guerra de squads):"
CUSTOM_FIELD_SQUAD_ID = 222343


# ------------------------- HTTP helpers ------------------------- #
def get_with_retry(url, params, tries=4, timeout=60):
    """GET com retry para chamadas 'lista' (erros != 404 devem falhar)."""
    err = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            err = e
            time.sleep(2 ** i)
    raise err


def get_detail_allow_404(url, params, tries=3, timeout=60):
    """GET com retry para chamadas 'detalhe' que podem retornar 404.
    Retorna: dict JSON se ok, None se 404, ou relança se outro erro."""
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            # Se não for 404, tenta novamente e no fim relança
            last = e
        except requests.RequestException as e:
            last = e
        time.sleep(2 ** i)
    # Se chegamos aqui e não foi 404, relança
    if last:
        raise last
    return None


# ------------------------- DB structure ------------------------- #
def ensure_structure(conn):
    ddl = """
    create schema if not exists visualizacao_agentes;

    create table if not exists visualizacao_agentes.agentes (
        agent_id     bigint primary key,
        name         text not null,
        email        text not null,
        team_primary text,
        teams        text[],
        access_type  text,
        is_active    boolean not null,
        raw          jsonb,
        updated_at   timestamptz not null default now(),
        time_squad   text
    );

    create index if not exists idx_agentes_email on visualizacao_agentes.agentes (lower(email));
    create index if not exists idx_agentes_team  on visualizacao_agentes.agentes (team_primary);
    create index if not exists idx_agentes_teams_gin on visualizacao_agentes.agentes using gin (teams);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


# ------------------------- API fetchers ------------------------- #
def fetch_all_agents():
    """Lista de pessoas (sem expand para evitar 400)."""
    results = []
    skip = 0
    top = 500
    while True:
        params = {
            "token": TOKEN,
            "$select": "id,businessName,userName,isActive,profileType,accessProfile",
            "$top": top,
            "$skip": skip,
        }
        r = get_with_retry(f"{BASE}/persons", params)
        page = r.json()
        if not isinstance(page, list):
            page = []
        results.extend(page)
        if len(page) < top:
            break
        skip += top
    return results


def fetch_person_detail(person_id):
    """Detalhe da pessoa com expand. Tolerante a 404 (retorna None)."""
    params = {"token": TOKEN, "$expand": "teams,customFieldValues,emails"}
    return get_detail_allow_404(f"{BASE}/persons/{person_id}", params)


# ------------------------- extract helpers ------------------------- #
def extract_email(detail, fallback_username):
    emails = (detail or {}).get("emails") or []
    for e in emails:
        if e.get("isDefault"):
            return e.get("email") or fallback_username or ""
    if emails:
        return emails[0].get("email") or fallback_username or ""
    return fallback_username or ""


def extract_teams(detail):
    teams = (detail or {}).get("teams") or []
    names = []
    for t in teams:
        n = t.get("name")
        if n:
            names.append(n)
    return names


def extract_time_squad(detail):
    """Lê o valor do campo adicional (lista de valores ou string simples)."""
    if not detail:
        return None
    vals = detail.get("customFieldValues") or []
    for v in vals:
        # Alguns retornam ID como string
        try:
            cfid = int(v.get("customFieldId"))
        except Exception:
            continue
        if cfid != CUSTOM_FIELD_SQUAD_ID:
            continue
        val = v.get("value")
        # Pode vir string, dict {name/value}, ou lista
        if isinstance(val, dict):
            if "name" in val:
                return str(val["name"])
            if "value" in val:
                return str(val["value"])
            return None
        if isinstance(val, list):
            # No Movidesk este campo é seleção única, mas mantemos seguro
            return ",".join([str(x) for x in val if x is not None]) or None
        return (None if val is None else str(val))
    return None


# ------------------------- upsert ------------------------- #
def upsert_agent(conn, row):
    sql = """
    insert into visualizacao_agentes.agentes
    (agent_id,name,email,team_primary,teams,access_type,is_active,raw,time_squad,updated_at)
    values
    (%(agent_id)s,%(name)s,%(email)s,%(team_primary)s,%(teams)s,%(access_type)s,%(is_active)s,%(raw)s,%(time_squad)s,now())
    on conflict (agent_id) do update set
      name=excluded.name,
      email=excluded.email,
      team_primary=excluded.team_primary,
      teams=excluded.teams,
      access_type=excluded.access_type,
      is_active=excluded.is_active,
      raw=excluded.raw,
      time_squad=excluded.time_squad,
      updated_at=now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)


# ------------------------- main ------------------------- #
def main():
    if not TOKEN or not DSN:
        raise RuntimeError("MOVIDESK_TOKEN e NEON_DSN são obrigatórios no ambiente")

    conn = psycopg2.connect(DSN)
    try:
        ensure_structure(conn)

        # Apenas perfis de agentes (1 e 3 são os usados no projeto)
        base_people = [p for p in fetch_all_agents() if p.get("profileType") in (1, 3)]

        for p in base_people:
            pid = p.get("id")
            if not pid:
                continue

            detail = fetch_person_detail(pid)
            # Se 404, apenas seguimos sem detalhe para esse ID
            teams = extract_teams(detail)
            team_primary = teams[0] if teams else None

            email = extract_email(detail, p.get("userName"))
            time_squad = extract_time_squad(detail)

            row = {
                "agent_id": int(pid),
                "name": p.get("businessName") or "",
                "email": email or "",
                "team_primary": team_primary,
                "teams": teams if teams else None,
                "access_type": p.get("accessProfile"),
                "is_active": bool(p.get("isActive")),
                "raw": Json(detail if detail is not None else p),  # guarda algo útil
                "time_squad": time_squad,
            }
            upsert_agent(conn, row)

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
