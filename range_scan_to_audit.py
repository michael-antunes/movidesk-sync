# -*- coding: utf-8 -*-
import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN   = os.getenv("NEON_DSN")

# Configurações
PAGE_TOP   = 100
LIMIT      = int(os.getenv("RANGE_SCAN_LIMIT", "400"))      # IDs por ciclo
THROTTLE   = float(os.getenv("RANGE_SCAN_THROTTLE", "0.25"))# pausa entre páginas
SLEEP_SEC  = int(os.getenv("RANGE_SCAN_SLEEP_SEC", "600"))  # 10 min entre ciclos

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

def z(dt: datetime) -> str:
    """Formata datetime em UTC para literal OData (YYYY-MM-DDTHH:MM:SSZ)."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_dt(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def conn():
    return psycopg2.connect(DSN)

def ensure_table():
    """Garante a tabela de controle e um registro inicial (2018-01-01 como data_fim)."""
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            create table if not exists visualizacao_resolvidos.range_scan_control(
              data_fim timestamptz not null,
              data_inicio timestamptz not null,
              ultima_data_validada timestamptz
            )
        """)
        cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
        if cur.fetchone()[0] == 0:
            # data_fim = 2018-01-01; data_inicio/ultima no "agora"
            cur.execute("""
                insert into visualizacao_resolvidos.range_scan_control
                    (data_fim, data_inicio, ultima_data_validada)
                values (timestamptz '2018-01-01 00:00:00+00', now(), now())
            """)

def fetch(params):
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:300]}")
    return r.json() or []

def read_bounds(cur):
    cur.execute("""
        select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio)
        from visualizacao_resolvidos.range_scan_control
        limit 1
    """)
    return cur.fetchone()

def start_or_get_run_id(cur, data_fim, ultima):
    """Pega o último run_id ou cria um novo run 'range-scan-loop'."""
    cur.execute("select max(id) from visualizacao_resolvidos.audit_recent_run")
    rid = cur.fetchone()[0]
    if rid is None:
        cur.execute("""
            insert into visualizacao_resolvidos.audit_recent_run
              (window_start, window_end, total_api, missing_total, run_at,
               window_from, window_to, total_local, notes)
            values (now(), now(), 0, 0, now(), %s, %s, 0, 'range-scan-loop')
            returning id
        """, (data_fim, ultima))
        rid = cur.fetchone()[0]
    return rid

def scan_cycle() -> bool:
    """
    Executa UM ciclo:
      - Busca até LIMIT tickets em [data_fim .. ultima_data_validada] (usando lastUpdate como janela)
      - Filtra por evento resolvido/fechado/cancelado (resolvedIn/closedIn/canceledIn)
      - Só insere na audit se NÃO existe em tickets_resolvidos
      - Move 'ultima_data_validada' para trás (min evento encontrado; se nada, salta direto para data_fim)
    Retorna:
      True  -> houve progresso (a janela andou para trás)
      False -> janela já finalizada (ultima<=data_fim) ou não há mais o que fazer
    """
    with conn() as c, c.cursor() as cur:
        data_inicio, data_fim, ultima = read_bounds(cur)

        # Condição de parada
        if ultima <= data_fim:
            print(f"[stop] ultima_data_validada ({ultima}) <= data_fim ({data_fim})")
            return False

        print(f"[range] varrendo de {ultima} para trás (limite {LIMIT})...")

        ids = []
        min_evt = None
        skip = 0

        while len(ids) < LIMIT:
            params = {
                "token": TOKEN,
                "$select": "id,baseStatus,resolvedIn,closedIn,canceledIn,lastUpdate",
                "$filter": f"lastUpdate ge {z(data_fim)} and lastUpdate le {z(ultima)}",
                "$orderby": "lastUpdate desc",
                "$top": PAGE_TOP,
                "$skip": skip
            }
            page = fetch(params)
            if not page:
                break

            for t in page:
                bs = (t.get("baseStatus") or "").strip()
                ev = None
                if bs in ("Resolved","Closed"):
                    ev = to_dt(t.get("resolvedIn")) or to_dt(t.get("closedIn"))
                elif bs == "Canceled":
                    ev = to_dt(t.get("canceledIn"))

                if not ev:
                    continue
                if ev < data_fim or ev > ultima:
                    continue

                try:
                    tid = int(t.get("id"))
                except Exception:
                    continue

                if tid not in ids:
                    ids.append(tid)
                if min_evt is None or ev < min_evt:
                    min_evt = ev

                if len(ids) >= LIMIT:
                    break

            if len(page) < PAGE_TOP:
                break

            skip += PAGE_TOP
            time.sleep(THROTTLE)

        # Se encontramos ids, filtra os que já existem em tickets_resolvidos
        missing = []
        rid = None
        if ids:
            cur.execute("""
                select ticket_id
                  from visualizacao_resolvidos.tickets_resolvidos
                 where ticket_id = any(%s)
            """, (ids,))
            have = {r[0] for r in cur.fetchall()}
            missing = [i for i in ids if i not in have]
            rid = start_or_get_run_id(cur, data_fim, ultima)

            if missing:
                execute_values(cur, """
                    insert into visualizacao_resolvidos.audit_recent_missing
                        (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                """, [(rid, "tickets_resolvidos", m) for m in missing])

        # Avança a “janela” (sempre indo para TRÁS)
        if min_evt:
            new_ultima = min_evt
        else:
            # se nada encontrado, saltamos direto para o limite inferior
            new_ultima = data_fim

        cur.execute("""
            update visualizacao_resolvidos.range_scan_control
               set ultima_data_validada = %s
        """, (new_ultima,))
        c.commit()

        print(f"[cycle] encontrados={len(ids)} (novos na audit={len(missing) if ids else 0}) | nova ultima={new_ultima}")

        # Progresso = conseguimos mover a janela (nova ultima < antiga ultima)
        progressed = new_ultima < ultima
        # Parar se agora já alcançou/ultrapassou o limite
        finished   = new_ultima <= data_fim
        return progressed and not finished

def main():
    ensure_table()
    # Laço “maratonista”: só para quando data_fim == ultima_data_validada
    while True:
        keep = False
        try:
            keep = scan_cycle()
        except Exception as e:
            # Falha de rede/transiente? loga e tenta novamente no próximo ciclo
            print(f"[warn] falha no ciclo: {e}")

        # Releitura rápida para decidir parada sem esperar
        with conn() as c, c.cursor() as cur:
            _, data_fim, ultima = read_bounds(cur)

        if ultima <= data_fim:
            print(f"[done] janela concluída: {ultima} <= {data_fim}")
            break

        if not keep:
            # Nada a fazer agora, mas ainda não chegou; dorme e tenta de novo
            print(f"[sleep] aguardando {SLEEP_SEC}s para próximo ciclo…")
            time.sleep(SLEEP_SEC)
        else:
            # Fez progresso; respeita intervalo entre varreduras
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
