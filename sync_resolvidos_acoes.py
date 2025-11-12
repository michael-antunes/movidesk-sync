#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza ações dos tickets resolvidos/fechados/cancelados.
- Processa 10 tickets por execução, olhando a fila em visualizacao_resolvidos.tickets_resolvidos
- Remove duplicatas antigas em tickets_resolvidos
- Busca ações por ticket (endpoint por ID) e faz UPSERT em visualizacao_resolvidos.resolvidos_acoes
Requisitos de ambiente:
  MOVIDESK_TOKEN   -> token da API Movidesk
  NEON_DSN         -> DSN Postgres (ex: postgres://user:pass@host/db)
Variáveis opcionais:
  MOVIDESK_THROTTLE -> sleep entre chamadas (default 0.25s)
  MOVIDESK_TOP      -> qtd de tickets por execução (default 10)
  HTTP_TIMEOUT      -> timeout requests (seg) (default 30)
"""
import os, time, json, sys
import psycopg2, psycopg2.extras
import requests

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN", "")
NEON_DSN  = os.getenv("NEON_DSN", "")
THROTTLE  = float(os.getenv("MOVIDESK_THROTTLE", "0.25"))
TOP       = int(os.getenv("MOVIDESK_TOP", "10"))
TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "30"))

# ---------- HTTP helper ----------
def req(url, params=None):
    """GET com retry simples e throttle."""
    if params is None:
        params = {}
    params = {k: v for k, v in params.items() if v is not None}
    for attempt in range(4):
        r = requests.get(url, params=params, timeout=TIMEOUT)
        if r.status_code >= 500:
            # backoff leve
            time.sleep(0.5 * (attempt + 1))
            continue
        r.raise_for_status()
        time.sleep(THROTTLE)
        return r.json()
    r.raise_for_status()

# ---------- DB helpers ----------
def conn():
    return psycopg2.connect(NEON_DSN)

def cleanup_dupes(c):
    """
    Remove duplicatas em visualizacao_resolvidos.tickets_resolvidos,
    mantendo somente o registro MAIS RECENTE por ticket_id
    (ordena por last_update desc; se não existir, cai no id desc).
    """
    sql = """
    with ranked as (
      select ctid,
             row_number() over(
               partition by ticket_id
               order by coalesce(last_update, resolvedin, closedin, canceledin) desc,
                        id desc
             ) rn
      from visualizacao_resolvidos.tickets_resolvidos
    )
    delete from visualizacao_resolvidos.tickets_resolvidos t
    using ranked r
    where t.ctid = r.ctid and r.rn > 1;
    """
    with c.cursor() as cur:
        cur.execute(sql)
    c.commit()

def next_ticket_ids(c, limit=10):
    """
    Seleciona até `limit` ticket_ids que precisam ter ações buscadas:
    - pega apenas a LINHA MAIS NOVA por ticket_id em tickets_resolvidos
    - filtra aqueles que ainda não existem em resolvidos_acoes
      ou que estão vazios (array json []).
    """
    sql = """
    with latest as (
      select *
      from (
        select tr.*,
               row_number() over(
                 partition by ticket_id
                 order by coalesce(last_update, resolvedin, closedin, canceledin) desc,
                          id desc
               ) rn
        from visualizacao_resolvidos.tickets_resolvidos tr
      ) x
      where x.rn = 1
    )
    select l.ticket_id
    from latest l
    left join visualizacao_resolvidos.resolvidos_acoes ra
           on ra.ticket_id = l.ticket_id
    where ra.ticket_id is null
       or coalesce(jsonb_array_length(ra.acoes), 0) = 0
    order by coalesce(l.last_update, l.resolvedin, l.closedin, l.canceledin) desc
    limit %s;
    """
    with c.cursor() as cur:
        cur.execute(sql, (limit,))
        return [row[0] for row in cur.fetchall()]

def upsert_acoes(c, rows):
    """
    rows: lista de tuplas (ticket_id, acoes_json)
    A tabela resolvidos_acoes tem UNIQUE(ticket_id).
    As colunas geradas (qtd_*) não são informadas no INSERT.
    """
    if not rows:
        return
    sql = """
    insert into visualizacao_resolvidos.resolvidos_acoes (ticket_id, acoes)
    values (%s, %s)
    on conflict (ticket_id) do update
      set acoes = excluded.acoes;
    """
    with c.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    c.commit()

def heartbeat(c):
    """Atualiza o relógio no sync_control.default (opcional/diagnóstico)."""
    sql = """
      insert into visualizacao_resolvidos.sync_control(name,last_update)
      values ('default', now())
      on conflict (name) do update set last_update = excluded.last_update
    """
    with c.cursor() as cur:
        cur.execute(sql)
    c.commit()

# ---------- Movidesk ----------
def fetch_actions_for_ticket(ticket_id: int):
    """
    Busca ações do ticket por ID.
    Estratégia:
      1) /tickets/{id}?$expand=actions(...): retorna 'description' (texto) e,
         de acordo com a base de conhecimento, pode retornar htmlDescription
         quando a consulta é por ID. Se vier, ótimo.
      2) Se html não vier, tentamos complementar via /tickets/htmldescription (best-effort).
    Campos salvos: id, isPublic, description, htmlDescription, createdDate, origin,
                   attachments (id,fileName), timeAppointments (id,workingTime)
    """
    # 1) por ID com expand das ações
    expand = "actions($expand=attachments,timeAppointments;$select=id,isPublic,description,createdDate,origin,attachments,timeAppointments)"
    data = req(f"{API_BASE}/tickets/{ticket_id}",
               {"token": API_TOKEN, "$select": "actions", "$expand": expand}) or {}
    actions = data.get("actions") or []

    # normaliza payload, pegando apenas campos úteis
    norm = []
    for a in actions:
        norm.append({
            "id": a.get("id"),
            "isPublic": a.get("isPublic"),
            "description": a.get("description"),
            "htmlDescription": a.get("htmlDescription"),  # pode vir quando busca é por ID
            "createdDate": a.get("createdDate"),
            "origin": a.get("origin"),
            "attachments": [
                {"id": att.get("id"), "fileName": att.get("fileName")}
                for att in (a.get("attachments") or [])
            ],
            "timeAppointments": [
                {"id": t.get("id"), "workingTime": t.get("workingTime")}
                for t in (a.get("timeAppointments") or [])
            ]
        })

    # 2) best-effort para HTML das ações (só se faltou):
    # documentação: htmlDescription só sai por ID ou pelo endpoint /tickets/htmldescription
    if any(x.get("htmlDescription") is None for x in norm):
        try:
            # tentamos os dois formatos comuns que já encontrei na prática:
            # a) /tickets/htmldescription/{id}
            html_payload = None
            try:
                html_payload = req(f"{API_BASE}/tickets/htmldescription/{ticket_id}",
                                   {"token": API_TOKEN})
            except requests.HTTPError:
                # b) /tickets/htmldescription?ticketId={id}
                html_payload = req(f"{API_BASE}/tickets/htmldescription",
                                   {"token": API_TOKEN, "ticketId": ticket_id})
            # se a resposta trouxer lista de ações com htmlDescription, fazemos o merge por id
            if isinstance(html_payload, dict) and "actions" in html_payload:
                html_actions = {a.get("id"): a.get("htmlDescription") for a in html_payload["actions"]}
                for item in norm:
                    if item.get("htmlDescription") is None and item.get("id") in html_actions:
                        item["htmlDescription"] = html_actions[item["id"]]
        except Exception:
            # se der qualquer erro aqui, seguimos apenas com description (texto)
            pass

    return norm

# ---------- Main ----------
def main():
    if not API_TOKEN or not NEON_DSN:
        print("ERRO: defina MOVIDESK_TOKEN e NEON_DSN no ambiente.", file=sys.stderr)
        sys.exit(1)

    c = conn()
    try:
        cleanup_dupes(c)
        ids = next_ticket_ids(c, TOP)
        if not ids:
            heartbeat(c)
            print("Nada para fazer.")
            return

        rows = []
        for tid in ids:
            try:
                acoes = fetch_actions_for_ticket(tid)
            except requests.HTTPError as e:
                # registra vazio para não travar; na próxima execução tentamos novamente
                print(f"ticket {tid}: HTTP {e}", file=sys.stderr)
                acoes = []
            rows.append((tid, json.dumps(acoes, ensure_ascii=False)))

        upsert_acoes(c, rows)
        heartbeat(c)
        print(f"Processados {len(rows)} tickets: {ids}")
    finally:
        c.close()

if __name__ == "__main__":
    main()
