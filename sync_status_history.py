#!/usr/bin/env python3
import os
import requests
import psycopg2
import json
from datetime import datetime, date

# --- CONFIGURAÇÃO ---
API_TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN       = os.getenv("NEON_DSN")
START_DATE = date.today().isoformat()  # busca só os tickets resolvidos/fechados a partir de hoje

# --- HELPERS ---
def fetch_tickets_with_history():
    """
    Busca todos os tickets cujo baseStatus seja 'Resolved' ou 'Closed' e que tenham
    sido resolvidos a partir de START_DATE, expandindo o history de status.
    """
    url = "https://api.movidesk.com/public/v1/tickets"
    params = {
        "token": API_TOKEN,
        "$select": "id,protocol,resolvedIn",
        "$filter": f"(baseStatus eq 'Resolved' or baseStatus eq 'Closed') and resolvedIn ge {START_DATE}",
        "$expand": (
            "statusHistories("
            "$select=status,justification,permanencyTimeWorkingTime,permanencyTimeFullTime,"
            "changedBy,changedDate)"
        )
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()

    # Alguns endpoints retornam {"items": [...]}, outros já retornam lista diretamente
    if isinstance(data, dict):
        return data.get("items", [])
    else:
        return data

def parse_iso(dt_str: str) -> datetime | None:
    """
    Transforma uma string ISO possivelmente com precisão >6 dígitos em um datetime válido.
    """
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        # se vier tipo '2025-07-29T18:00:12.2248568'
        if "." in dt_str:
            main, frac = dt_str.split(".", 1)
            frac = frac[:6]  # pega só microssegundos
            try:
                return datetime.fromisoformat(f"{main}.{frac}")
            except ValueError:
                pass
    return None

# --- GRAVAÇÃO NO DB ---
def save_history(records: list[dict]):
    """
    Insere ou atualiza cada registro de status na tabela visualizacao_resolucao.resolucao_por_status
    """
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SET search_path TO visualizacao_resolucao;")

    sql = """
    INSERT INTO resolucao_por_status (
      ticket_id,
      protocol,
      status,
      justificativa,
      seconds_utl,
      permanency_time_fulltime_seconds,
      changed_by,
      changed_date,
      imported_at
    ) VALUES (
      %(ticket_id)s,
      %(protocol)s,
      %(status)s,
      %(justification)s,
      %(seconds_utl)s,
      %(perm_seconds)s,
      %(changed_by)s,
      %(changed_date)s,
      NOW()
    )
    ON CONFLICT (ticket_id, status, justificativa) DO UPDATE
      SET seconds_utl                        = EXCLUDED.seconds_utl,
          permanency_time_fulltime_seconds   = EXCLUDED.permanency_time_fulltime_seconds,
          changed_by                         = EXCLUDED.changed_by,
          changed_date                       = EXCLUDED.changed_date,
          imported_at                        = NOW();
    """

    for ticket in records:
        tid   = ticket.get("id")
        proto = ticket.get("protocol")
        for hist in ticket.get("statusHistories", []):
            status      = hist.get("status")
            justification = hist.get("justification") or "-"
            # --- MAPEAMENTO CORRETO ---
            # horas corridas (inclusive o tempo parado)
            seconds_utl = hist.get("permanencyTimeFullTime")
            # horas úteis (descontado tempo parado)
            perm_seconds = hist.get("permanencyTimeWorkingTime")

            changed_by_raw = hist.get("changedBy")
            changed_by = json.dumps(changed_by_raw) if changed_by_raw else None

            changed_date = None
            cd_str = hist.get("changedDate")
            if cd_str:
                changed_date = parse_iso(cd_str)

            cur.execute(sql, {
                "ticket_id": tid,
                "protocol": proto,
                "status": status,
                "justification": justification,
                "seconds_utl": seconds_utl,
                "perm_seconds": perm_seconds,
                "changed_by": changed_by,
                "changed_date": changed_date
            })

    conn.commit()
    cur.close()
    conn.close()

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    tickets = fetch_tickets_with_history()
    save_history(tickets)
