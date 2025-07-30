#!/usr/bin/env python3
import os
import json
import time
import requests
import psycopg2
from datetime import datetime

# -----------------------------------------------------------------------------
# Parâmetros de ambiente obrigatórios
# -----------------------------------------------------------------------------
TOKEN = os.getenv("MOVIDESK_TOKEN")
DSN   = os.getenv("NEON_DSN")
if not TOKEN or not DSN:
    raise RuntimeError("É preciso definir as variáveis MOVIDESK_TOKEN e NEON_DSN")

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------
HEADERS  = {"token": TOKEN}
BASE_URL = "https://api.movidesk.com/public/v1"

# -----------------------------------------------------------------------------
# Busca a lista de tickets que já foram resolvidos (estão na tabela movidesk_resolution)
# -----------------------------------------------------------------------------
def get_ticket_ids():
    sql = """
      SELECT DISTINCT ticket_id
        FROM visualizacao_resolucao.movidesk_resolution
      WHERE ticket_id IS NOT NULL
    """
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]

# -----------------------------------------------------------------------------
# Chama a API de histórico de status de um ticket
# -----------------------------------------------------------------------------
def fetch_status_history(ticket_id):
    url = f"{BASE_URL}/tickets/statusHistory"
    params = {"ticketId": ticket_id}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        print(f"[ERROR] Ticket {ticket_id}: status {resp.status_code} → {resp.text}")
        return []
    data = resp.json()
    return data.get("items", data if isinstance(data, list) else [])

# -----------------------------------------------------------------------------
# Persiste no banco cada evento de status
# -----------------------------------------------------------------------------
def save_history(entries):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            for e in entries:
                ticket_id    = e.get("ticketId")
                status       = e.get("status")
                justification= e.get("justification") or "-"
                seconds_utl  = e.get("secondsWorkingTime")
                full_seconds = e.get("workingTimeFulltimeSeconds")
                changed_by   = e.get("changedBy")
                changed_date = e.get("changedDate")
                agent_name   = e.get("agentName")
                team_name    = e.get("teamName")

                # Converte changed_date de string ISO para datetime
                dt = None
                if changed_date:
                    try:
                        dt = datetime.fromisoformat(changed_date)
                    except ValueError:
                        dt = datetime.fromisoformat(changed_date[:26])

                cur.execute("""
                    INSERT INTO visualizacao_resolucao.resolucao_por_status
                      (ticket_id, protocol, status, justificativa,
                       seconds_utl, permanency_time_fulltime_seconds,
                       changed_by, changed_date,
                       agent_name, team_name)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (ticket_id, status, justificativa) DO NOTHING
                """, (
                    ticket_id,
                    None,  # protocol não usamos aqui
                    status,
                    justification,
                    seconds_utl,
                    full_seconds,
                    json.dumps(changed_by) if changed_by is not None else None,
                    dt,
                    agent_name,
                    team_name,
                ))
        conn.commit()

# -----------------------------------------------------------------------------
# Fluxo principal
# -----------------------------------------------------------------------------
def main():
    print("Iniciando sync_status_history.py")
    tickets = get_ticket_ids()
    print(f"→ {len(tickets)} tickets na fila de histórico")
    all_events = []
    for tid in tickets:
        evs = fetch_status_history(tid)
        print(f"  Ticket {tid}: {len(evs)} eventos")
        all_events.extend(evs)
        time.sleep(0.2)
    print(f"Gravando {len(all_events)} eventos no banco")
    save_history(all_events)
    print("Concluído com sucesso.")

if __name__ == "__main__":
    main()
