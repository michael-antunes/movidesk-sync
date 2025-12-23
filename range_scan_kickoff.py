import os
import psycopg2

DSN = os.environ["NEON_DSN"]
FALLBACK_END = os.getenv("KICK_FALLBACK_END", "2018-01-01 00:00:00+00")

with psycopg2.connect(DSN) as c, c.cursor() as cur:
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
        cur.execute("select %s::timestamptz", (FALLBACK_END,))
        min_lu = cur.fetchone()[0]

    if max_id is None or min_id is None:
        cur.execute("select min(ticket_id), max(ticket_id) from dados_gerais.tickets_suporte")
        min_id2, max_id2 = cur.fetchone()
        if min_id is None:
            min_id = min_id2
        if max_id is None:
            max_id = max_id2

    cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
    exists = cur.fetchone()[0] != 0

    if not exists:
        cur.execute(
            """
            insert into visualizacao_resolvidos.range_scan_control
              (data_inicio, data_fim, ultima_data_validada, id_inicial, id_final, id_atual)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (max_lu, min_lu, max_lu, min_id, max_id, max_id),
        )
    else:
        cur.execute(
            """
            update visualizacao_resolvidos.range_scan_control
               set data_inicio = %s,
                   data_fim = %s,
                   ultima_data_validada = %s,
                   id_inicial = %s,
                   id_final = %s,
                   id_atual = %s
            """,
            (max_lu, min_lu, max_lu, min_id, max_id, max_id),
        )
    c.commit()

print("Kickoff OK: range_scan_control atualizado.")
