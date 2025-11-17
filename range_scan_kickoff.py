# -*- coding: utf-8 -*-
import os, psycopg2

DSN = os.environ["NEON_DSN"]
KICK_END = "2018-01-01 00:00:00+00"  # data_fim desejada

with psycopg2.connect(DSN) as c, c.cursor() as cur:
    cur.execute("create schema if not exists visualizacao_resolvidos")
    cur.execute("""
        create table if not exists visualizacao_resolvidos.range_scan_control(
          data_fim timestamptz not null,
          data_inicio timestamptz not null,
          ultima_data_validada timestamptz,
          constraint ck_range_scan_bounds check (
            (data_inicio is not null) and (data_fim is not null) and (data_inicio <> data_fim) and
            (ultima_data_validada is null or (
              ultima_data_validada >= least(data_inicio, data_fim) and
              ultima_data_validada <= greatest(data_inicio, data_fim)
            ))
          )
        )
    """)
    # Se não existe linha, cria; se existe, reseta para iniciar o ciclo
    cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            insert into visualizacao_resolvidos.range_scan_control
              (data_inicio, data_fim, ultima_data_validada)
            values (now(), %s::timestamptz, now())
        """, (KICK_END,))
    else:
        cur.execute("""
            update visualizacao_resolvidos.range_scan_control
               set data_inicio = now(),
                   data_fim = %s::timestamptz,
                   ultima_data_validada = now()
        """, (KICK_END,))
    c.commit()

print("Kickoff OK: range_scan_control inicializado.")
