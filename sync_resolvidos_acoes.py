#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import json
import logging
import requests
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)7s  %(message)s"
)

API_BASE = os.getenv("MOVIDESK_API_BASE", "https://api.movidesk.com/public/v1")
API_TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")
NEON_DSN = os.getenv("NEON_DSN")

SCHEMA_AUD = "visualizacao_resolvidos"
SCHEMA_DST = "visualizacao_resolvidos"
T_AUDIT = f"{SCHEMA_AUD}.audit_recent_missing"
T_ACOES = f"{SCHEMA_DST}.resolvidos_acoes"

CHAT_ORIGINS = ("5", "6", "23", "25", "26", "27")

IDS_LIMIT = int(os.getenv("MOVIDESK_ACOES_LIMIT", "100"))
THROTTLE_SEC = float(os.getenv("MOVIDESK_THROTTLE", "3.0"))

if not API_TOKEN or not NEON_DSN:
    raise RuntimeError("Defina MOVIDESK_TOKEN/MOVIDESK_API_TOKEN e NEON_DSN")

SESS = requests.Session()
SESS.headers.update({"User-Agent": "movidesk-sync/resolvidos-acoes"})


def _sleep_retry_after(r):
    try:
        ra = r.headers.get("retry-after")
        if ra:
            time.sleep(max(1, int(str(ra).strip())))
            return True
    except Exception:
        pass
    return False


def md_get(params, max_retries=4):
    p = dict(params or {})
    p["token"] = API_TOKEN
    last_err = None
    for i in range(max_retries):
        r = SESS.get(f"{API_BASE}/tickets", params=p, timeout=60)
        if r.status_code == 200:
            try:
                js = r.json() or []
            except ValueError:
                return []
            return js
        if r.status_code in (429, 500, 502, 503, 504):
            if _sleep_retry_after(r):
                continue
            time.sleep(1 + 2 * i)
            last_err = requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
            continue
        r.raise_for_status()
    if last_err:
        last_err.response.raise_for_status()
    return []


# --------- PARTE 1: leitura / limpeza da AUDIT ----------

def get_audit_ids(conn, limit_):
    sql = f"""
      select ticket_id
        from {T_AUDIT}
       where table_name = 'resolvidos_acoes'
       order by run_id desc, ticket_id desc
       limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit_,))
        rows = cur.fetchall() or []
    ids = [r[0] for r in rows]
    if ids:
        logging.info(
            "Lote da AUDIT (resolvidos_acoes): %s%s",
            ids[:10],
            " ..." if len(ids) > 10 else ""
        )
    return ids


def clear_audit_ids(conn, ids):
    if not ids:
        return
    sql = f"""
      delete from {T_AUDIT}
       where table_name = 'resolvidos_acoes'
         and ticket_id = any(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ids,))
    conn.commit()
    logging.info("Limpou %d ticket(s) da audit (resolvidos_acoes).", len(ids))


# --------- PARTE 2: fallback (sem audit) ----------

def get_recent_chat_ids(conn, limit_):
    sql = f"""
        select tr.ticket_id
        from visualizacao_resolvidos.tickets_resolvidos tr
        join visualizacao_resolvidos.resolvidos_acoes ra
          on ra.ticket_id = tr.ticket_id
        where tr.origin in %s
        order by tr.ticket_id desc
        limit %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (CHAT_ORIGINS, limit_))
        rows = cur.fetchall() or []
    ids = [r[0] for r in rows]
    if ids:
        logging.info(
            "Lote RECENTE (fallback, chat/Whats): %s%s",
            ids[:10],
            " ..." if len(ids) > 10 else ""
        )
    return ids


# --------- PARTE 3: chamada na API / update ----------

def fetch_ticket_actions_with_html(ticket_id):
    params = {
        "id": ticket_id,
        "$select": "id,actions",
        "$expand": (
            "actions("
            "$select=id,origin,type,status,createdDate,createdBy,"
            "description,htmlDescription,attachments)"
        ),
    }
    data = md_get(params) or []
    if isinstance(data, list):
        ticket = data[0] if data else {}
    else:
        ticket = data if isinstance(data, dict) else {}
    actions = ticket.get("actions") or []
    return actions


def update_resolvidos_acoes(conn, ticket_id, actions):
    payload = json.dumps(actions, ensure_ascii=False)
    sql = f"""
        update {T_ACOES}
           set acoes = %s,
               updated_at = now()
         where ticket_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (payload, ticket_id))


def process_ids(conn, ticket_ids):
    if not ticket_ids:
        return 0
    updated = 0
    for idx, ticket_id in enumerate(ticket_ids, start=1):
        try:
            actions = fetch_ticket_actions_with_html(ticket_id)
            update_resolvidos_acoes(conn, ticket_id, actions)
            updated += 1
            logging.info(
                "RESOLVIDOS_ACOES: ticket %s atualizado com %d ação(ões)",
                ticket_id,
                len(actions),
            )
        except Exception as exc:
            logging.warning("RESOLVIDOS_ACOES ERRO ticket %s: %s", ticket_id, exc)
        if idx < len(ticket_ids):
            time.sleep(THROTTLE_SEC)
    conn.commit()
    return updated


# --------- PARTE 4: main ----------

def main():
    conn = psycopg2.connect(NEON_DSN)
    try:
        # 1) tenta consumir primeiro a fila da AUDIT
        ids = get_audit_ids(conn, IDS_LIMIT)
        used_audit = bool(ids)

        if not ids:
            # 2) se audit estiver vazia, usa o fallback (tickets recentes)
            ids = get_recent_chat_ids(conn, IDS_LIMIT)
            if not ids:
                logging.info("Nenhum ticket para atualizar em resolvidos_acoes.")
                return

        updated = process_ids(conn, ids)
        logging.info("RESOLVIDOS_ACOES: %d ticket(s) atualizados.", updated)

        # 3) se veio da audit, limpa os IDs processados
        if used_audit and ids:
            clear_audit_ids(conn, ids)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
