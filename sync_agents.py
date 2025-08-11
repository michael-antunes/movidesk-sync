import os
import time
import json
import requests
import psycopg
from typing import List, Dict, Any

MOVIDESK_BASE = "https://api.movidesk.com/public/v1"
TOKEN = os.environ["MOVIDESK_TOKEN"]
PG_URL = os.environ["NEON_POSTGRES_URL"]

# ---------- helpers de requisição (com paginação básica) ----------
def fetch_paginated(endpoint: str, base_params: Dict[str, Any] = None, page_size: int = 100) -> List[Dict[str, Any]]:
    params = dict(base_params or {})
    params["token"] = TOKEN
    params["$top"] = page_size

    all_items: List[Dict[str, Any]] = []
    skip = 0
    while True:
        params["$skip"] = skip
        r = requests.get(f"{MOVIDESK_BASE}/{endpoint}", params=params, timeout=60)
        if r.status_code == 429:
            # respeita rate limit simples
            time.sleep(5)
            continue
        r.raise_for_status()
        batch = r.json()
        if not isinstance(batch, list):
            # quando a API devolve um objeto único
            batch = [batch] if batch else []
        all_items.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return all_items

# Tenta obter o máximo possível direto dos agents; complementa por teams caso precise
def fetch_agents() -> List[Dict[str, Any]]:
    # Dica: muitos endpoints do Movidesk suportam OData ($select/$expand).
    # Se seu tenant permitir, você pode destravar mais campos com $select/$expand.
    try:
        return fetch_paginated("agents", base_params={})
    except requests.HTTPError:
        # fallback defensivo (raro)
        return []

def fetch_teams() -> List[Dict[str, Any]]:
    try:
        return fetch_paginated("teams", base_params={})
    except requests.HTTPError:
        return []

def build_team_index(teams_payload: List[Dict[str, Any]]) -> Dict[int, List[str]]:
    """
    Constrói um índice: agent_id -> [nomes_de_equipes]
    Usa a lista de teams; cada team geralmente contém seus 'agents' ou 'members'.
    """
    index: Dict[int, List[str]] = {}
    for t in teams_payload:
        team_name = t.get("name") or t.get("title") or "Sem equipe"
        members = t.get("agents") or t.get("members") or []
        # membros podem ser lista de ids ou objetos
        for m in members:
            if isinstance(m, dict):
                aid = m.get("id") or m.get("agentId")
            else:
                aid = m
            if aid is None:
                continue
            index.setdefault(int(aid), []).append(team_name)
    return index

def extract_email(agent: Dict[str, Any]) -> str:
    # Alguns tenants retornam email direto; outros aninham em "person"
    return (
        agent.get("email")
        or (agent.get("person") or {}).get("email")
        or (agent.get("businessEmail"))
        or ""
    )

def extract_access_type(agent: Dict[str, Any]) -> str:
    # Campos possíveis/variantes
    return (
        agent.get("businessProfile")
        or agent.get("profileType")
        or agent.get("accessType")
        or (agent.get("profile") or {}).get("type")
        or ""
    )

def extract_is_active(agent: Dict[str, Any]) -> bool:
    v = agent.get("isActive")
    if v is None:
        v = agent.get("active")
    if isinstance(v, bool):
        return v
    # casos onde vem "true"/"false" como string
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "sim")
    return bool(v)

def normalize_agents(agents_payload: List[Dict[str, Any]],
                     team_index: Dict[int, List[str]]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for a in agents_payload:
        agent_id = a.get("id") or a.get("agentId")
        if agent_id is None:
            # ignora payload estranho
            continue
        agent_id = int(agent_id)

        name = a.get("name") or (a.get("person") or {}).get("name") or ""
        email = extract_email(a)
        access_type = extract_access_type(a)
        is_active = extract_is_active(a)

        # tenta pegar equipes do próprio payload do agente (quando existe)
        raw_teams = a.get("teams") or a.get("groups") or []
        teams_names = []
        if raw_teams:
            # pode vir lista de strings, ids ou objetos
            for t in raw_teams:
                if isinstance(t, dict):
                    teams_names.append(t.get("name") or t.get("title"))
                elif isinstance(t, str):
                    teams_names.append(t)
        # complementa com índice vindo do /teams
        if not teams_names and agent_id in team_index:
            teams_names = team_index[agent_id]

        teams_names = [t for t in teams_names if t] or None
        team_primary = teams_names[0] if teams_names else None

        norm.append({
            "agent_id": agent_id,
            "name": name,
            "email": email,
            "team_primary": team_primary,
            "teams": teams_names,
            "access_type": access_type,
            "is_active": is_active,
            "raw": a
        })
    return norm

def ensure_table(conn: psycopg.Connection):
    ddl = """
    CREATE SCHEMA IF NOT EXISTS visualizacao_agentes;

    CREATE TABLE IF NOT EXISTS visualizacao_agentes.agentes (
      agent_id     BIGINT PRIMARY KEY,
      name         TEXT NOT NULL,
      email        TEXT NOT NULL,
      team_primary TEXT,
      teams        TEXT[],
      access_type  TEXT,
      is_active    BOOLEAN NOT NULL,
      raw          JSONB,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_agentes_email ON visualizacao_agentes.agentes (lower(email));
    CREATE INDEX IF NOT EXISTS idx_agentes_team ON visualizacao_agentes.agentes (team_primary);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def upsert_agents(conn: psycopg.Connection, rows: List[Dict[str, Any]]):
    sql = """
    INSERT INTO visualizacao_agentes.agentes
      (agent_id, name, email, team_primary, teams, access_type, is_active, raw, updated_at)
    VALUES
      (%(agent_id)s, %(name)s, %(email)s, %(team_primary)s, %(teams)s, %(access_type)s, %(is_active)s, %(raw)s, now())
    ON CONFLICT (agent_id) DO UPDATE SET
      name = EXCLUDED.name,
      email = EXCLUDED.email,
      team_primary = EXCLUDED.team_primary,
      teams = EXCLUDED.teams,
      access_type = EXCLUDED.access_type,
      is_active = EXCLUDED.is_active,
      raw = EXCLUDED.raw,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

def main():
    print("Baixando agents…")
    agents_payload = fetch_agents()

    print("Baixando teams (para mapear equipes)…")
    teams_payload = fetch_teams()
    team_index = build_team_index(teams_payload)

    print("Normalizando…")
    rows = normalize_agents(agents_payload, team_index)

    print(f"{len(rows)} agentes prontos para upsert.")
    with psycopg.connect(PG_URL, autocommit=False) as conn:
        ensure_table(conn)
        upsert_agents(conn, rows)
    print("Concluído.")

if __name__ == "__main__":
    main()
