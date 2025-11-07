#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reprocessa os tickets que ficaram faltando (audit_recent_missing) e
preenche as tabelas de destino:
  - visualizacao_resolvidos.tickets_resolvidos
  - visualizacao_resolvidos.resolvidos_acoes      (coluna 'acoes' JSONB)
  - visualizacao_resolvidos.detail_control

Estratégia:
  1) Lê o último run em visualizacao_resolvidos.audit_recent_run
  2) Busca os ticket_ids em visualizacao_resolvidos.audit_recent_missing
  3) Para cada tabela alvo, carrega da API e faz UPSERT dinâmico
     (apenas colunas existentes na tabela são usadas)
  4) Usa $expand=actions para buscar ações (evita 404)
"""

import os
import time
import json
import math
import logging
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests


API_BASE = "https://api.movidesk.com/public/v1"
SCHEMA = "visualizacao_resolvidos"
TBL_RUN = f"{SCHEMA}.audit_recent_run"
TBL_MISS = f"{SCHEMA}.audit_recent_missing"

# Tabelas de destino
TBL_TICKETS = f"{SCHEMA}.tickets_resolvidos"
TBL_ACOES   = f"{SCHEMA}.resolvidos_acoes"
TBL_DETAIL  = f"{SCHEMA}.detail_control"

# Configuração básico de log
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(message)s"
)
log = logging.getLogger("reprocess_missing")


def env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    if not v and not default:
        raise RuntimeError(f"Env var {name} não definida")
    return v


def db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(env("NEON_DSN"))


def throttling():
    try:
        t = float(os.getenv("MOVIDESK_THROTTLE", "0"))
        if t > 0:
            time.sleep(t)
    except Exception:
        pass


def chunks(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ---------- API MOVIDESK ----------

def md_get(url: str, params: Dict[str, Any]) -> requests.Response:
    throttling()
    r = requests.get(url, params=params, timeout=60)
    return r


def fetch_tickets_raw(ids: List[int]) -> List[Dict[str, Any]]:
    """
    Busca tickets por lista de ids (até ~80 por chamada) usando $filter in.
    Retorna a lista bruta (cada item é um ticket).
    """
    if not ids:
        return []
    url = f"{API_BASE}/tickets"
    # IMPORTANTÍSSIMO: id é numérico -> sem aspas
    filter_in = ",".join(str(i) for i in ids)
    params = {
        "token": env("MOVIDESK_TOKEN"),
        "$filter": f"id in ({filter_in})",
        # Campos amplos o suficiente para alimentar as tabelas destino
        "$select": (
            "id,subject,origin,baseStatus,status,owner,ownerId,ownerTeam,ownerTeamId,"
            "organization,organizationId,organizationName,"
            "resolvedIn,closedIn,lastActionDate,actionCount"
        )
    }
    r = md_get(url, params)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    data = r.json() or []
    return data


def fetch_ticket_actions(ticket_id: int) -> List[Dict[str, Any]]:
    """
    Busca ações do ticket usando $expand=actions.
    Retorna a lista (pode ser vazia).
    """
    url = f"{API_BASE}/tickets"
    params = {
        "token": env("MOVIDESK_TOKEN"),
        "$filter": f"id eq {ticket_id}",
        "$select": "id",
        "$expand": (
            "actions("
            "$select=id,origin,createdDate,baseStatus,description,isPublic,createdBy,createdByTeam"
            ")"
        ),
    }
    r = md_get(url, params)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text}")
    data = r.json() or []
    if not data:
        return []
    return data[0].get("actions") or []


# ---------- POSTGRES HELPERS ----------

def get_table_columns(cur, schema: str, table: str) -> List[str]:
    cur.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s and table_name = %s
        order by ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def upsert_rows(
    cur,
    schema: str,
    table: str,
    rows: List[Dict[str, Any]],
    conflict_cols: Sequence[str]
):
    """
    UPSERT dinâmico: usa só colunas que realmente existem na tabela.
    conflict_cols: chaves para ON CONFLICT.
    """
    if not rows:
        return

    table_cols = set(get_table_columns(cur, schema, table))
    # União de todas as chaves que teremos *e* que existem na tabela
    used_cols = []
    for k in rows[0].keys():
        if k in table_cols:
            used_cols.append(k)

    if not used_cols:
        log.warning("Nenhuma coluna aplicável para %s.%s; ignorado", schema, table)
        return

    # Monta SQL com placeholders
    cols_sql = ", ".join(used_cols)
    vals_sql = ", ".join(["%s"] * len(used_cols))
    conflict_sql = ", ".join(conflict_cols)

    updates = [f'{c} = EXCLUDED.{c}' for c in used_cols if c not in conflict_cols]
    set_sql = ", ".join(updates) if updates else ""

    sql = f"INSERT INTO {schema}.{table} ({cols_sql}) VALUES ({vals_sql})"
    if conflict_cols:
        sql += f" ON CONFLICT ({conflict_sql}) DO "
        sql += ("UPDATE SET " + set_sql) if set_sql else "NOTHING"

    args = []
    for r in rows:
        values = [r.get(c) for c in used_cols]
        args.append(values)

    psycopg2.extras.execute_batch(cur, sql, args, page_size=200)
    log.info("UPSERT %s.%s -> %d linhas", schema, table, len(rows))


# ---------- TRANSFORMAÇÕES ----------

def to_ticket_row(t: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mapeia um ticket bruto em colunas genéricas (o upsert só gravará as que existirem).
    """
    return {
        # chaves comuns
        "ticket_id": t.get("id"),
        # responsável
        "responsible_id": t.get("ownerId"),
        "responsible_name": t.get("owner"),
        # origem/status
        "origin": t.get("origin"),
        "status": t.get("status"),
        "base_status": t.get("baseStatus"),
        # organização
        "organization_id": t.get("organizationId"),
        "organization_name": t.get("organizationName") or t.get("organization"),
        # datas úteis
        "resolved_in": t.get("resolvedIn"),
        "closed_in": t.get("closedIn"),
        "last_action_date": t.get("lastActionDate"),
        # contagem
        "tickets": 1,
        "action_count": t.get("actionCount"),
        # raw opcional
        "raw": json.dumps(t),
    }


def to_detail_row(t: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticket_id": t.get("id"),
        "resolved_in": t.get("resolvedIn"),
        "closed_in": t.get("closedIn"),
        "last_action_date": t.get("lastActionDate"),
        "action_count": t.get("actionCount"),
        "synced_at": psycopg2.TimestampFromTicks(time.time()),
    }


def to_acoes_row(ticket_id: int, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Grava as ações em JSONB (coluna 'acoes'). Colunas geradas (qtd_*) na tabela
    serão recalculadas automaticamente e não são tocadas aqui.
    """
    # Limpa só o necessário
    acoes_slim = []
    for a in actions or []:
        acoes_slim.append({
            "id": a.get("id"),
            "origin": a.get("origin"),
            "createdDate": a.get("createdDate"),
            "baseStatus": a.get("baseStatus"),
            "description": a.get("description"),
            "isPublic": a.get("isPublic"),
            "createdBy": a.get("createdBy"),
            "createdByTeam": a.get("createdByTeam"),
        })
    return {
        "ticket_id": ticket_id,
        "acoes": json.dumps(acoes_slim, ensure_ascii=False),
        "synced_at": psycopg2.TimestampFromTicks(time.time()),
    }


# ---------- PIPELINE ----------

def resolve_run_id(cur) -> int:
    forced = os.getenv("RUN_ID")
    if forced:
        cur.execute(f"select id from {TBL_RUN} where id = %s", (int(forced),))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"RUN_ID={forced} não existe em {TBL_RUN}")
        return int(forced)
    cur.execute(f"select id from {TBL_RUN} order by id desc limit 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Nenhum run em {TBL_RUN}")
    return int(row[0])


def load_missing(cur, run_id: int) -> Dict[str, List[int]]:
    cur.execute(
        f"""
        select table_name, ticket_id
        from {TBL_MISS}
        where run_id = %s
        order by table_name, ticket_id
        """,
        (run_id,),
    )
    groups: Dict[str, List[int]] = {}
    for tbl, tid in cur.fetchall():
        groups.setdefault(tbl, []).append(int(tid))
    return groups


def process_tickets(cur, ids: List[int]):
    """
    Preenche tickets_resolvidos (e, de quebra, detail_control) para o conjunto de ids informado.
    """
    if not ids:
        return
    log.info("Buscando %d tickets (em lotes)...", len(ids))
    all_tickets: List[Dict[str, Any]] = []
    for batch in chunks(ids, 80):
        data = fetch_tickets_raw(batch)
        all_tickets.extend(data)

    # UPSERT tickets_resolvidos
    rows_tickets = [to_ticket_row(t) for t in all_tickets]
    upsert_rows(cur, SCHEMA, "tickets_resolvidos", rows_tickets, conflict_cols=["ticket_id"])

    # UPSERT detail_control (aproveita a mesma carga)
    rows_detail = [to_detail_row(t) for t in all_tickets]
    upsert_rows(cur, SCHEMA, "detail_control", rows_detail, conflict_cols=["ticket_id"])


def process_acoes(cur, ids: List[int]):
    """
    Preenche resolvidos_acoes (coluna 'acoes' JSONB) para os ids.
    """
    if not ids:
        return
    log.info("Buscando ações de %d tickets (1 a 1 com $expand=actions)...", len(ids))
    rows: List[Dict[str, Any]] = []
    for tid in ids:
        try:
            acts = fetch_ticket_actions(tid)
        except Exception as e:
            log.warning("Falha ao buscar ações do ticket %s: %s", tid, e)
            acts = []
        rows.append(to_acoes_row(tid, acts))

    upsert_rows(cur, SCHEMA, "resolvidos_acoes", rows, conflict_cols=["ticket_id"])


def main():
    token = env("MOVIDESK_TOKEN")
    del token  # só para forçar validação; usamos via env() nas funções

    with db_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            run_id = resolve_run_id(cur)
            log.info("RUN_ID alvo: %s", run_id)

            groups = load_missing(cur, run_id)
            log.info("Pendências por tabela: %s", {k: len(v) for k, v in groups.items()})

            # tickets_resolvidos
            ids_tk = groups.get("tickets_resolvidos", [])
            if ids_tk:
                process_tickets(cur, ids_tk)

            # detail_control (se houver ids isolados só para ela)
            ids_dc = groups.get("detail_control", [])
            if ids_dc:
                # reaproveita a mesma rotina de tickets (pega do endpoint de tickets)
                process_tickets(cur, ids_dc)

            # resolvidos_acoes
            ids_ac = groups.get("resolvidos_acoes", [])
            if ids_ac:
                process_acoes(cur, ids_ac)

        conn.commit()
    log.info("Reprocesso finalizado com sucesso.")


if __name__ == "__main__":
    main()
