# audit_recent_resolved_scan.py
# -*- coding: utf-8 -*-

import os
import time
import math
import json
import datetime as dt
from typing import Iterable, List, Dict, Any, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values


API_URL = "https://api.movidesk.com/public/v1/tickets"
# Campos mínimos: só precisamos dos IDs + carimbos para filtrar
API_FIELDS = "id,baseStatus,resolvedIn,closedIn"
PAGE_SIZE = 1000  # conforme pedido
THROTTLE = float(os.environ.get("MOVIDESK_THROTTLE", "0.25"))  # segundos entre chamadas


def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Variável de ambiente obrigatória ausente: {name}")
    return v


def ensure_sql_objects(cur) -> None:
    """
    Garante as tabelas/índices/constraints exatamente como você tem hoje.
    Também acerta DEFAULT 0 para missing_total (evita violar NOT NULL no insert do run).
    """
    cur.execute("""
    create schema if not exists visualizacao_resolvidos;

    create table if not exists visualizacao_resolvidos.audit_recent_run (
      id            bigserial primary key,
      started_at    timestamptz not null default now(),
      window_start  timestamptz not null,
      window_end    timestamptz not null,
      total_api     integer not null,
      missing_total integer not null default 0
    );

    -- Garante DEFAULT 0 caso a tabela já exista sem default
    alter table visualizacao_resolvidos.audit_recent_run
      alter column missing_total set default 0;

    create table if not exists visualizacao_resolvidos.audit_recent_missing (
      run_id     bigint not null
                 references visualizacao_resolvidos.audit_recent_run(id)
                 on delete cascade,
      table_name text   not null,
      ticket_id  integer not null
    );

    -- Evita duplicar a mesma ausência na mesma execução
    create unique index if not exists ux_audit_recent_missing
      on visualizacao_resolvidos.audit_recent_missing(run_id, table_name, ticket_id);
    """)

    # Opcional: checa a existência das três tabelas de destino (apenas warning se não houver)
    cur.execute("""
      select n.nspname as schema, c.relname as table
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
      where c.relkind = 'r'
        and n.nspname = 'visualizacao_resolvidos'
        and c.relname in ('tickets_resolvidos','resolvidos_acoes','detail_control')
      order by c.relname;
    """)
    found = {row[1] for row in cur.fetchall()}
    expected = {'tickets_resolvidos','resolvidos_acoes','detail_control'}
    missing = expected - found
    if missing:
        print("[WARN] Tabelas não encontradas no schema visualizacao_resolvidos:", ", ".join(sorted(missing)))


def movidesk_get_tickets(token: str, date_from: dt.datetime, date_to: dt.datetime) -> List[int]:
    """
    Busca IDs de tickets que tenham resolvedIn OU closedIn dentro do intervalo [date_from, date_to].
    """
    ids: List[int] = []

    # Movidesk aceita filtros com 'resolvedIn ge ... and resolvedIn le ...' etc.
    # Se fechado ou resolvido na janela, queremos.
    # Observação: API do Movidesk aceita pageSize e page (1-based). Usamos 1000 como você pediu.
    df = date_from.isoformat()
    dt_ = date_to.isoformat()

    # Dois filtros: resolvedIn e closedIn. Mesclamos resultados.
    filters = [
        f"(resolvedIn ge {json.dumps(df)} and resolvedIn le {json.dumps(dt_)})",
        f"(closedIn   ge {json.dumps(df)} and closedIn   le {json.dumps(dt_)})"
    ]

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json; charset=utf-8"})

    for flt in filters:
        page = 1
        while True:
            params = {
                "token": token,
                "$select": API_FIELDS,
                "$filter": flt,
                "pageSize": PAGE_SIZE,
                "page": page
            }
            resp = session.get(API_URL, params=params, timeout=60)
            if resp.status_code != 200:
                raise RuntimeError(f"Movidesk HTTP {resp.status_code}: {resp.text}")

            data = resp.json()
            if not isinstance(data, list):
                # Quando a API devolve objeto com 'message' etc.
                raise RuntimeError(f"Resposta inesperada da API Movidesk: {data}")

            if not data:
                break

            new_ids = [int(x["id"]) for x in data if "id" in x]
            ids.extend(new_ids)

            print(f"[Movidesk] filtro={flt} | page={page} | recebidos={len(new_ids)}")
            if len(new_ids) < PAGE_SIZE:
                break
            page += 1
            time.sleep(THROTTLE)

    # remove duplicados (ticket pode aparecer nos dois filtros)
    ids = sorted(set(ids))
    print(f"[Movidesk] total único de IDs no período: {len(ids)}")
    return ids


def split_chunks(seq: List[int], size: int = 500) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


def find_missing(cur, table: str, ids: List[int]) -> List[Tuple[str, int]]:
    """
    Retorna pares (table, ticket_id) que estão faltando na tabela informada.
    table ∈ {'tickets_resolvidos','resolvidos_acoes','detail_control'}
    """
    missing: List[Tuple[str, int]] = []
    if not ids:
        return missing

    # Monta checagem em lotes para não estourar o limite de parâmetros
    for chunk in split_chunks(ids, 5000):
        q = f"""
        with src(id) as (
          select unnest(%s::int[])
        )
        select s.id
        from src s
        left join visualizacao_resolvidos.{table} t
          on t.ticket_id = s.id
        where t.ticket_id is null
        """
        cur.execute(q, (chunk,))
        rows = cur.fetchall()
        for (ticket_id,) in rows:
            missing.append((table, int(ticket_id)))
    return missing


def main() -> None:
    token = _env("MOVIDESK_TOKEN")
    dsn = _env("NEON_DSN")

    # Janela: últimos 2 dias (UTC). Ajuste se quiser timezone local.
    now = dt.datetime.now(dt.timezone.utc)
    window_end = now
    window_start = now - dt.timedelta(days=2)

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Garante estruturas
            ensure_sql_objects(cur)
            conn.commit()

        # 1) Busca IDs na API (resolvidos ou fechados)
        ids = movidesk_get_tickets(token, window_start, window_end)
        total_api = len(ids)

        with conn.cursor() as cur:
            # 2) Abre um "run" com missing_total=0 (tem DEFAULT 0)
            cur.execute("""
                insert into visualizacao_resolvidos.audit_recent_run
                  (window_start, window_end, total_api, missing_total)
                values (%s, %s, %s, 0)
                returning id
            """, (window_start, window_end, total_api))
            run_id = cur.fetchone()[0]
            print(f"[RUN] id={run_id} total_api={total_api}")

            # 3) Checa ausências nas três tabelas
            missing_all: List[Tuple[str, int]] = []
            for table in ("tickets_resolvidos", "resolvidos_acoes", "detail_control"):
                miss = find_missing(cur, table, ids)
                print(f"[MISSING] {table}: {len(miss)} ausentes")
                missing_all.extend(miss)

            # 4) Insere em audit_recent_missing (deduplicado pelo índice único)
            if missing_all:
                execute_values(
                    cur,
                    """
                    insert into visualizacao_resolvidos.audit_recent_missing
                      (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                    """,
                    [(run_id, t, i) for (t, i) in missing_all],
                    template="(%s,%s,%s)"
                )

            # 5) Atualiza o missing_total do run com o que foi parar em audit_recent_missing
            cur.execute("""
                update visualizacao_resolvidos.audit_recent_run r
                   set missing_total = coalesce(m.cnt, 0)
                from (
                  select run_id, count(*)::int as cnt
                  from visualizacao_resolvidos.audit_recent_missing
                  where run_id = %s
                  group by 1
                ) m
                where r.id = %s
            """, (run_id, run_id))

        conn.commit()
        print("[OK] Execução concluída.")


if __name__ == "__main__":
    main()
