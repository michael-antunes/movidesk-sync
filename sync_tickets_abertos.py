drop view if exists visualizacao_atual.vw_tickets_abertos_painel;

create view visualizacao_atual.vw_tickets_abertos_painel as
with
  agg as (
    select
      s.ticket_id,
      max(s.empresa_id::text)                  as empresa_id,
      max(s.empresa_nome)                     as empresa_nome,
      max(s.codereferenceadditional)          as codereferenceadditional,
      max(s.cpfcnpj)                          as cpfcnpj,
      max(s.responsavel)                      as responsavel,
      max(s.equipe_responsavel)               as equipe_responsavel,
      max(s.categoria)                        as categoria,
      max(s.urgencia)                         as urgencia,
      max(s.created_date)                     as created_date,
      max(s.resolved_date)                    as resolved_date,
      max(s.closed_date)                      as closed_date,
      max(s.canceled_date)                    as canceled_date,
      sum(s.tempo_corridos_segundos)          as tempo_corridos_segundos,
      sum(s.tempo_corridos_segundos) / 60.0   as tempo_corridos_minutos,
      sum(s.tempo_corridos_segundos) / 3600.0 as tempo_corridos_horas,
      sum(s.uteis_segundos)                   as uteis_segundos,
      sum(s.uteis_segundos) / 60.0            as uteis_minutos,
      sum(s.uteis_segundos) / 3600.0          as uteis_horas
    from
      visualizacao_atual.vw_tickets_abertos_tempo_por_status s
    group by
      s.ticket_id
  )
select
  a.ticket_id,
  a.empresa_id,
  a.empresa_nome,
  a.codereferenceadditional,
  a.cpfcnpj,
  a.responsavel,
  a.equipe_responsavel,
  a.categoria,
  a.urgencia,
  a.created_date,
  a.resolved_date,
  a.closed_date,
  a.canceled_date,
  a.tempo_corridos_segundos,
  a.tempo_corridos_minutos,
  a.tempo_corridos_horas,
  to_char(
    (a.tempo_corridos_segundos::bigint || ' seconds')::interval,
    'HH24:MI:SS'
  ) as tempo_corridos_hhmmss,
  a.uteis_segundos,
  a.uteis_minutos,
  a.uteis_horas,
  to_char(
    (a.uteis_segundos::bigint || ' seconds')::interval,
    'HH24:MI:SS'
  ) as uteis_hhmmss,
  -- aqui só espelhamos o que já está em visualizacao_atual.tickets_abertos
  ta.reaberturas,
  1 as cont
from
  agg a
  left join visualizacao_atual.tickets_abertos ta
    on ta.ticket_id = a.ticket_id;
