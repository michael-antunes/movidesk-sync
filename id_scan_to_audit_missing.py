import os
import psycopg2
from psycopg2.extras import execute_values

DSN = os.getenv("NEON_DSN")
BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "1"))
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

if not DSN:
    raise RuntimeError("NEON_DSN não definido")

def ensure_control(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.range_scan_control(
          data_fim timestamptz not null,
          data_inicio timestamptz not null,
          ultima_data_validada timestamptz,
          id_inicial bigint,
          id_final bigint,
          id_atual bigint,
          constraint ck_range_scan_bounds check (
            (data_inicio is not null) and (data_fim is not null) and (data_inicio <> data_fim) and
            (ultima_data_validada is null or (
              ultima_data_validada >= least(data_inicio, data_fim) and
              ultima_data_validada <= greatest(data_inicio, data_fim)
            ))
          )
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_inicial bigint")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_final bigint")
    cur.execute("alter table visualizacao_resolvidos.range_scan_control add column if not exists id_atual bigint")

def init_control_row(cur):
    cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
    if cur.fetchone()[0] != 0:
        return

    cur.execute(
        """
        select
          min(last_update) filter (where last_update is not null) as min_lu,
          max(last_update) filter (where last_update is not null) as max_lu,
          min(ticket_id) as min_id,
          max(ticket_id) as max_id
        from visualizacao_resolvidos.tickets_resolvidos_detail
        """
    )
    min_lu, max_lu, min_id, max_id = cur.fetchone()

    if max_lu is None:
        cur.execute("select now()")
        max_lu = cur.fetchone()[0]
    if min_lu is None:
        cur.execute("select now() - interval '1 day'")
        min_lu = cur.fetchone()[0]

    if max_id is None or min_id is None:
        cur.execute("select min(ticket_id), max(ticket_id) from dados_gerais.tickets_suporte")
        min_id2, max_id2 = cur.fetchone()
        if min_id is None:
            min_id = min_id2
        if max_id is None:
            max_id = max_id2

    cur.execute(
        """
        insert into visualizacao_resolvidos.range_scan_control
          (data_inicio, data_fim, ultima_data_validada, id_inicial, id_final, id_atual)
        values (%s, %s, %s, %s, %s, %s)
        """,
        (max_lu, min_lu, max_lu, min_id, max_id, max_id),
    )

def ensure_missing_tables(cur):
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          started_at timestamptz not null default now(),
          window_start timestamptz not null,
          window_end timestamptz not null,
          total_api int not null,
          missing_total int not null default 0,
          run_at timestamptz,
          window_from timestamptz,
          window_to timestamptz,
          total_local int,
          notes text
        )
        """
    )
    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint not null references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )

def create_run(cur, notes):
    cur.execute(
        """
        insert into visualizacao_resolvidos.audit_recent_run(window_start, window_end, total_api, missing_total, notes)
        values (now(), now(), 0, 0, %s)
        returning id
        """,
        (notes,),
    )
    return int(cur.fetchone()[0])

def fetch_control(cur):
    cur.execute(
        """
        select id_inicial, id_final, id_atual
        from visualizacao_resolvidos.range_scan_control
        limit 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]

def update_control(cur, id_inicial, id_final, id_atual):
    cur.execute(
        """
        update visualizacao_resolvidos.range_scan_control
           set id_inicial = %s,
               id_final = %s,
               id_atual = %s
        """,
        (id_inicial, id_final, id_atual),
    )

def ids_exist_tickets_suporte(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from dados_gerais.tickets_suporte
        where ticket_id = any(%s)
        """,
        (ids,),
    )
    return {int(r[0]) for r in cur.fetchall()}

def ids_in_mesclados(cur, ids):
    if not ids:
        return set()
    cur.execute(
        """
        select ticket_id
        from visualizacao_resolvidos.tickets_mesclados
        where ticket_id = any(%s) or merged_into_id = any(%s)
        """,
        (ids, ids),
    )
    return {int(r[0]) for r in cur.fetchall()}

def insert_missing(cur, run_id, ids):
    if not ids:
        return 0
    execute_values(
        cur,
        """
        insert into visualizacao_resolvidos.audit_recent_missing(run_id, table_name, ticket_id)
        values %s
        on conflict do nothing
        """,
        [(run_id, TABLE_NAME, int(t)) for t in ids],
    )
    return len(ids)

def main():
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        ensure_control(cur)
        init_control_row(cur)
        ensure_missing_tables(cur)
        run_id = create_run(cur, "id-scan: range_scan_control ids -> audit_recent_missing (skip mesclados)")
        c.commit()

        total_checked = 0
        total_missing = 0

        for _ in range(max(1, ITERATIONS)):
            id_inicial, id_final, id_atual = fetch_control(cur)
            if id_inicial is None or id_final is None:
                break
            if id_atual is None:
                id_atual = id_final
                update_control(cur, id_inicial, id_final, id_atual)
                c.commit()

            if id_atual < id_inicial:
                break

            start_id = int(id_atual)
            end_id = int(max(id_inicial, id_atual - BATCH_SIZE + 1))
            batch = list(range(start_id, end_id - 1, -1))
            total_checked += len(batch)

            exist = ids_exist_tickets_suporte(cur, batch)
            mesclados = ids_in_mesclados(cur, batch)
            missing = [i for i in batch if i not in exist and i not in mesclados]

            total_missing += insert_missing(cur, run_id, missing)

            next_id = end_id - 1
            update_control(cur, id_inicial, id_final, next_id)
            c.commit()

        cur.execute(
            """
            update visualizacao_resolvidos.audit_recent_run
               set window_end = now(),
                   total_api = %s,
                   missing_total = %s
             where id = %s
            """,
            (total_checked, total_missing, run_id),
        )
        c.commit()

if __name__ == "__main__":
    main()
