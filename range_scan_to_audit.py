# -*- coding: utf-8 -*-
import os, time, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

API = "https://api.movidesk.com/public/v1/tickets"
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN   = os.getenv("NEON_DSN")

PAGE_TOP  = 100
LIMIT     = int(os.getenv("RANGE_SCAN_LIMIT", "400"))
THROTTLE  = float(os.getenv("RANGE_SCAN_THROTTLE", "0.25"))
# limite de execução (ex.: 240s) e pausa entre ciclos (deixe 0 p/ não dormir)
MAX_RUNTIME_SEC = int(os.getenv("RANGE_SCAN_MAX_RUNTIME_SEC", "240"))
SLEEP_SEC       = float(os.getenv("RANGE_SCAN_SLEEP_SEC", "0"))

if not TOKEN or not DSN:
    raise RuntimeError("MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN são obrigatórios")

def z(dt):
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
    with conn() as c, c.cursor() as cur:
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
        cur.execute("select count(*) from visualizacao_resolvidos.range_scan_control")
        if cur.fetchone()[0] == 0:
            # inicializa com seu default “semanal reverso”
            cur.execute("""
                insert into visualizacao_resolvidos.range_scan_control
                  (data_inicio, data_fim, ultima_data_validada)
                values (now(), timestamptz '2018-01-01 00:00:00+00', now())
            """)

def fetch(params):
    r = requests.get(API, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Movidesk HTTP {r.status_code}: {r.text[:300]}")
    return r.json() or []

def do_one_cycle():
    """
    Executa UM ciclo de varredura (respeita LIMIT e paginação) e
    avança ultima_data_validada até o evento mais antigo encontrado no ciclo.
    Retorna (atingiu_fim: bool, qtd_ids_encontrados: int).
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select data_inicio, data_fim, coalesce(ultima_data_validada, data_inicio)
            from visualizacao_resolvidos.range_scan_control
            limit 1
        """)
        data_inicio, data_fim, ultima = cur.fetchone()

    # Se já chegou no limite inferior, nada a fazer
    if ultima <= data_fim:
        return True, 0

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
            # garante que o evento está dentro da janela alvo
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

    missing = []
    with conn() as c, c.cursor() as cur:
        if ids:
            # filtra os que ainda não existem na tabela final
            cur.execute("""
                select ticket_id
                from visualizacao_resolvidos.tickets_resolvidos
                where ticket_id = any(%s)
            """, (ids,))
            have = {r[0] for r in cur.fetchall()}
            missing = [i for i in ids if i not in have]

            # garante um run_id para auditar a inserção
            cur.execute("select max(id) from visualizacao_resolvidos.audit_recent_run")
            rid = cur.fetchone()[0]
            if rid is None:
                cur.execute("""
                    insert into visualizacao_resolvidos.audit_recent_run
                      (window_start, window_end, total_api, missing_total, run_at, window_from, window_to, total_local, notes)
                    values (now(), now(), 0, 0, now(), %s, %s, 0, 'range-scan-semanal') returning id
                """, (data_fim, ultima))
                rid = cur.fetchone()[0]

            if missing:
                execute_values(cur, """
                    insert into visualizacao_resolvidos.audit_recent_missing (run_id, table_name, ticket_id)
                    values %s
                    on conflict do nothing
                """, [(rid, "tickets_resolvidos", m) for m in missing])

        # avança o ponteiro de progresso
        if min_evt:
            nv = min_evt
        else:
            nv = data_fim  # se não achou nada novo, crava no fim para encerrar no próximo teste

        cur.execute("""
            update visualizacao_resolvidos.range_scan_control
               set ultima_data_validada = %s
        """, (nv,))
        c.commit()

    # atingiu limite?
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            select (coalesce(ultima_data_validada, data_inicio) <= data_fim)
            from visualizacao_resolvidos.range_scan_control
            limit 1
        """)
        hit_end = bool(cur.fetchone()[0])

    return hit_end, len(ids)

def main():
    ensure_table()
    start = time.time()

    while True:
        hit_end, got = do_one_cycle()
        print(f"[range-scan] ciclo: inseriu {got} em audit; hit_end={hit_end}")

        # encerra se já chegou ao fim da janela
        if hit_end:
            print("[range-scan] FIM: ultima_data_validada já alcançou data_fim.")
            break

        # respeita o teto de runtime do job
        elapsed = time.time() - start
        if elapsed >= MAX_RUNTIME_SEC:
            print(f"[range-scan] tempo esgotado ({elapsed:.1f}s >= {MAX_RUNTIME_SEC}s). Encerrando este job.")
            break

        # pequena pausa opcional entre ciclos
        if SLEEP_SEC > 0:
            time.sleep(min(SLEEP_SEC, max(0, MAX_RUNTIME_SEC - (time.time() - start))))

if __name__ == "__main__":
    main()
