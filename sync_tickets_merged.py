#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, time, json, requests, psycopg2
from psycopg2.extras import execute_values

API_BASE = "https://api.movidesk.com/public/v1"
TOKEN    = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
DSN      = os.getenv("NEON_DSN")
BATCH    = int(os.getenv("MERGED_BATCH", "200"))       # quantos itens buscar por passada no fallback
THROTTLE = float(os.getenv("THROTTLE_SEC", "0.25"))

if not TOKEN or not DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

S = requests.Session()
S.headers.update({"User-Agent":"movidesk-sync/merged"})

def md_get(path_or_full, params=None, ok_404=False):
    url = path_or_full if path_or_full.startswith("http") else f"{API_BASE}/{path_or_full}"
    p = dict(params or {})
    p["token"] = TOKEN
    r = S.get(url, params=p, timeout=60)
    if r.status_code == 200:
        return r.json() or []
    if ok_404 and r.status_code == 404:
        return None
    # tolera throttling/eventuais
    if r.status_code in (429,500,502,503,504):
        time.sleep(1.5)
        r2 = S.get(url, params=p, timeout=60)
        if r2.status_code == 200:
            return r2.json() or []
    r.raise_for_status()

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists visualizacao_resolvidos.tickets_mesclados(
          ticket_id integer primary key,
          merged_into_id integer,
          merged_at timestamptz,
          situacao_mesclado text generated always as ('Sim') stored,
          raw_payload jsonb,
          imported_at timestamptz default now()
        )""")
        cur.execute("create index if not exists ix_tk_merged_into on visualizacao_resolvidos.tickets_mesclados(merged_into_id)")
    conn.commit()

def upsert_rows(conn, rows):
    if not rows: return
    sql = """
    insert into visualizacao_resolvidos.tickets_mesclados
      (ticket_id, merged_into_id, merged_at, raw_payload, imported_at)
    values %s
    on conflict (ticket_id) do update set
      merged_into_id = excluded.merged_into_id,
      merged_at      = coalesce(excluded.merged_at, visualizacao_resolvidos.tickets_mesclados.merged_at),
      raw_payload    = excluded.raw_payload,
      imported_at    = now()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()

# ---------- 1) TENTATIVA COM ENDPOINT DEDICADO ----------
def try_fetch_dedicated(conn):
    """
    Tenta usar o endpoint oficial de merges.
    Como a KB é privada, aceitamos mais de um formato de chave.
    """
    try:
        # Sem paginação explícita — ajuste se a sua conta tiver muitos merges históricos
        data = md_get("tickets/merged", params={"$orderby":"mergedDate desc", "$top": 1000}, ok_404=True)
        if data is None:
            return False  # 404
        rows = []
        for it in data or []:
            # normaliza campos mais comuns
            src = it.get("sourceId") or it.get("ticketId") or it.get("source") or it.get("fromId")
            dst = it.get("targetId") or it.get("mergedIntoId") or it.get("target") or it.get("toId")
            dt  = it.get("mergedDate") or it.get("performedAt") or it.get("date")
            try:
                src = int(src) if src is not None else None
                dst = int(dst) if dst is not None else None
            except Exception:
                continue
            if not src:
                continue
            rows.append((src, dst, dt, json.dumps(it, ensure_ascii=False)))
        upsert_rows(conn, rows)
        return True
    except requests.HTTPError as e:
        # 400/403/…: segue para fallback
        print(f"[WARN] dedicated endpoint not available ({e}). Using fallback by histories.")
        return False

# ---------- 2) FALLBACK VIA statusHistories ----------
JUSTIF_RX = re.compile(r"(mescl|merge|unid|duplic)", re.I)
TARGET_ID_RX = re.compile(r"(?:#|n[ºo]\s*|id\s*:?|ticket\s*:?|protocolo\s*:?)[^\d]*(\d{3,})", re.I)

def get_a_candidate_ids(conn, limit):
    """Pega candidatos a 'mesclado' ainda não mapeados (ex.: IDs que deram 404 nos seus jobs)."""
    # Estratégia simples: olhamos a audit_recent_missing + ainda não existem em tickets_mesclados
    with conn.cursor() as cur:
        cur.execute("""
           select distinct m.ticket_id
             from visualizacao_resolvidos.audit_recent_missing m
        left join visualizacao_resolvidos.tickets_mesclados tm
               on tm.ticket_id = m.ticket_id
            where m.table_name in ('tickets_resolvidos','resolucao_por_status')
              and tm.ticket_id is null
         order by m.ticket_id desc
            limit %s
        """, (limit,))
        return [r[0] for r in cur.fetchall()]

def fetch_histories_for(ids):
    if not ids: return []
    filtro = " or ".join([f"id eq {i}" for i in ids])
    params = {
        "$select": "id",
        "$filter": filtro,
        "$expand": "statusHistories($select=status,justification,changedDate)"
    }
    data = md_get("tickets", params)
    time.sleep(THROTTLE)
    return data or []

def extract_merge_from_histories(item):
    tid = item.get("id")
    best_dt, target = None, None
    for h in item.get("statusHistories") or []:
        just = (h.get("justification") or "")[:400]
        if not JUSTIF_RX.search(just or ""):
            continue
        # tenta achar ticket destino na justificativa (#12345 etc.)
        m = TARGET_ID_RX.search(just or "")
        if m:
            try:
                target = int(m.group(1))
            except Exception:
                pass
        # guarda a data mais recente de justificativa que parece merge
        dt = h.get("changedDate") or h.get("date")
        if dt and (best_dt is None or str(dt) > str(best_dt)):
            best_dt = dt
    if tid and (best_dt or target):
        return int(tid), target, best_dt
    return None

def run_fallback(conn):
    ids = get_a_candidate_ids(conn, BATCH)
    if not ids:
        return
    data = fetch_histories_for(ids)
    rows = []
    for it in data:
        got = extract_merge_from_histories(it)
        if got:
            src, dst, dt = got
            rows.append((src, dst, dt, json.dumps(it, ensure_ascii=False)))
    upsert_rows(conn, rows)

def main():
    with psycopg2.connect(DSN) as conn:
        ensure_table(conn)
        ok = try_fetch_dedicated(conn)
        if not ok:
            run_fallback(conn)
        print("tickets_mesclados: sincronização concluída.")

if __name__ == "__main__":
    main()
