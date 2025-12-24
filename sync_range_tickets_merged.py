#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import psycopg2
import psycopg2.extras


# -----------------------------
# Config / Helpers
# -----------------------------

def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    if v is None or v.strip() == "":
        return default
    return v.strip()

def env_int(key: str, default: int) -> int:
    v = env_str(key)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default

def parse_movidesk_dt(s: Optional[str]) -> Optional[datetime]:
    """
    Manual do Movidesk costuma retornar 'YYYY-MM-DD HH:MM:SS' (sem timezone).
    Para evitar bagunça de fuso, vamos assumir UTC.
    """
    if not s:
        return None
    s = s.strip()
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


@dataclass
class Settings:
    # Movidesk
    movi_base_url: str
    movi_token: str
    movi_rpm: int  # requests per minute (<=10 recomendado)

    # DB
    db_schema: str
    control_table: str
    merged_table: str

    # Job behavior
    batch_size: int
    dry_run: bool
    http_timeout: int


def load_settings() -> Settings:
    token = env_str("MOVI_TOKEN")
    if not token:
        raise RuntimeError("Falta MOVI_TOKEN (token da API Movidesk).")

    return Settings(
        movi_base_url=env_str("MOVI_BASE_URL", "https://api.movidesk.com/public/v1"),
        movi_token=token,
        movi_rpm=max(1, min(env_int("MOVI_RPM", 9), 10)),  # clamp 1..10

        db_schema=env_str("DB_SCHEMA", "visualizacao_resolvidos"),
        control_table=env_str("CONTROL_TABLE", "range_scan_control"),
        merged_table=env_str("MERGED_TABLE", "tickets_mesclados"),

        batch_size=max(1, env_int("BATCH_SIZE", 80)),
        dry_run=(env_str("DRY_RUN", "false").lower() in ("1", "true", "yes", "y")),
        http_timeout=max(5, env_int("HTTP_TIMEOUT", 30)),
    )


# -----------------------------
# Postgres
# -----------------------------

def pg_connect():
    """
    Aceita DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD(/PGPORT).
    """
    db_url = env_str("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    host = env_str("PGHOST")
    dbname = env_str("PGDATABASE")
    user = env_str("PGUSER")
    password = env_str("PGPASSWORD")
    port = env_str("PGPORT", "5432")

    if not (host and dbname and user and password):
        raise RuntimeError("Faltam variáveis de Postgres. Use DATABASE_URL ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=int(port) if port else 5432,
        connect_timeout=15,
    )


def ensure_tables(conn, cfg: Settings) -> None:
    """
    Cria schema/tabelas (se não existirem) e garante colunas mínimas.
    A ideia aqui é ser idempotente e não quebrar o que você já tem.
    """
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{cfg.db_schema}";')

        # Controle (compatível com seu print)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{cfg.db_schema}"."{cfg.control_table}" (
                data_inicio           timestamptz NULL,
                ultima_data_validada  timestamptz NULL,
                data_fim              timestamptz NULL,
                id_inicial            bigint NULL,
                id_final              bigint NULL,
                id_atual              bigint NULL,
                id_atual_merged       bigint NULL
            );
        """)

        # Tabela de merges: 1 linha por ticketId com os IDs mesclados (string ';')
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{cfg.db_schema}"."{cfg.merged_table}" (
                ticket_id          bigint PRIMARY KEY,
                merged_tickets     integer NULL,
                merged_tickets_ids text NULL,
                last_update        timestamptz NULL,
                created_at         timestamptz NOT NULL DEFAULT now(),
                updated_at         timestamptz NOT NULL DEFAULT now()
            );
        """)

        # Garantir colunas (caso sua tabela já exista com outro shape)
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
        """, (cfg.db_schema, cfg.merged_table))
        cols = {r[0] for r in cur.fetchall()}

        def add_col(sql: str):
            cur.execute(sql)

        if "merged_tickets" not in cols:
            add_col(f'ALTER TABLE "{cfg.db_schema}"."{cfg.merged_table}" ADD COLUMN merged_tickets integer NULL;')
        if "merged_tickets_ids" not in cols:
            add_col(f'ALTER TABLE "{cfg.db_schema}"."{cfg.merged_table}" ADD COLUMN merged_tickets_ids text NULL;')
        if "last_update" not in cols:
            add_col(f'ALTER TABLE "{cfg.db_schema}"."{cfg.merged_table}" ADD COLUMN last_update timestamptz NULL;')
        if "created_at" not in cols:
            add_col(f'ALTER TABLE "{cfg.db_schema}"."{cfg.merged_table}" ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();')
        if "updated_at" not in cols:
            add_col(f'ALTER TABLE "{cfg.db_schema}"."{cfg.merged_table}" ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();')

    conn.commit()


def read_control(conn, cfg: Settings) -> Dict[str, Any]:
    """
    Lê a “linha de controle”. Assumimos que sua tabela tem 1 linha (como no print).
    Se tiver mais de uma, ele pega a mais recente por data_inicio.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT *
            FROM "{cfg.db_schema}"."{cfg.control_table}"
            ORDER BY data_inicio DESC NULLS LAST
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"Nenhuma linha encontrada em {cfg.db_schema}.{cfg.control_table}. "
                "Crie 1 registro com id_inicial/id_final/id_atual_merged."
            )
        return dict(row)


def update_control_pointer(conn, cfg: Settings, next_id: int, finished: bool) -> None:
    """
    Atualiza id_atual_merged + timestamps.
    Atualiza todas as linhas (assumindo tabela singleton). Se preferir, depois a gente refina com PK.
    """
    with conn.cursor() as cur:
        if finished:
            cur.execute(f"""
                UPDATE "{cfg.db_schema}"."{cfg.control_table}"
                SET id_atual_merged = %s,
                    ultima_data_validada = now(),
                    data_fim = COALESCE(data_fim, now());
            """, (next_id,))
        else:
            cur.execute(f"""
                UPDATE "{cfg.db_schema}"."{cfg.control_table}"
                SET id_atual_merged = %s,
                    ultima_data_validada = now();
            """, (next_id,))
    conn.commit()


def upsert_merged_rows(conn, cfg: Settings, rows: List[Tuple[int, Optional[int], Optional[str], Optional[datetime]]]) -> int:
    """
    rows: (ticket_id, merged_tickets, merged_tickets_ids, last_update_dt)
    """
    if not rows:
        return 0

    now_dt = datetime.now(timezone.utc)
    values = []
    for (ticket_id, merged_tickets, merged_tickets_ids, last_update_dt) in rows:
        values.append((
            int(ticket_id),
            merged_tickets,
            merged_tickets_ids,
            last_update_dt,
            now_dt,
        ))

    sql = f"""
        INSERT INTO "{cfg.db_schema}"."{cfg.merged_table}"
            (ticket_id, merged_tickets, merged_tickets_ids, last_update, updated_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
            merged_tickets     = EXCLUDED.merged_tickets,
            merged_tickets_ids = EXCLUDED.merged_tickets_ids,
            last_update        = EXCLUDED.last_update,
            updated_at         = EXCLUDED.updated_at;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(rows)


# -----------------------------
# Movidesk Client
# -----------------------------

class MovideskClient:
    def __init__(self, base_url: str, token: str, rpm: int, timeout: int):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()

        # intervalo mínimo entre requests para respeitar rpm
        self.min_interval_s = 60.0 / max(1, rpm)
        self._last_req_ts = 0.0

    def _throttle(self):
        now = time.time()
        elapsed = now - self._last_req_ts
        if elapsed < self.min_interval_s:
            time.sleep(self.min_interval_s - elapsed)
        self._last_req_ts = time.time()

    def get_ticket_merged(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        """
        Chama GET /tickets/merged

        O manual pode aceitar 'id' (número/protocolo) e retorna:
          { ticketId, mergedTickets, mergedTicketsIds, lastUpdate }

        Se não existir merge para o ticket, normalmente retorna 404.
        """
        self._throttle()
        url = f"{self.base_url}/tickets/merged"

        # tenta com 'id' (manual mais comum)
        params = {"token": self.token, "id": str(ticket_id)}

        r = self.session.get(url, params=params, timeout=self.timeout)

        # Alguns ambientes podem usar 'ticketId' como nome do parâmetro — fallback
        if r.status_code in (400, 404):
            # tenta outra variação só se falhar
            params2 = {"token": self.token, "ticketId": str(ticket_id)}
            r2 = self.session.get(url, params=params2, timeout=self.timeout)
            # se a segunda der certo, usa ela
            if r2.status_code < 400:
                r = r2

        if r.status_code == 404:
            return None

        # 429 / 503: backoff simples
        if r.status_code in (429, 503):
            time.sleep(5)
            return self.get_ticket_merged(ticket_id)

        r.raise_for_status()

        data = r.json()
        # em alguns casos pode vir lista; normaliza para 1 dict (quando busca por ticket)
        if isinstance(data, list):
            if not data:
                return None
            data = data[0]
        if not isinstance(data, dict):
            return None
        return data


def normalize_merged_payload(payload: Dict[str, Any]) -> Optional[Tuple[int, Optional[int], Optional[str], Optional[datetime]]]:
    """
    Retorna: (ticket_id, merged_tickets, merged_tickets_ids_str, last_update_dt)
    Se não houver merge, retorna None.
    """
    ticket_id_raw = payload.get("ticketId") or payload.get("id")
    if not ticket_id_raw:
        return None

    try:
        ticket_id = int(str(ticket_id_raw))
    except ValueError:
        return None

    merged_tickets_raw = payload.get("mergedTickets")
    merged_tickets: Optional[int] = None
    if merged_tickets_raw is not None:
        try:
            merged_tickets = int(str(merged_tickets_raw))
        except ValueError:
            merged_tickets = None

    merged_ids = payload.get("mergedTicketsIds")
    merged_ids_str = str(merged_ids).strip() if merged_ids is not None else None

    # Se não tiver ids mesclados ou for zero, trata como “não mesclado”
    if not merged_ids_str:
        return None
    if merged_tickets is not None and merged_tickets <= 0:
        return None

    last_update_dt = parse_movidesk_dt(payload.get("lastUpdate"))
    return (ticket_id, merged_tickets, merged_ids_str, last_update_dt)


# -----------------------------
# Main
# -----------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | tickets_merged_range | %(message)s",
    )

    cfg = load_settings()

    conn = pg_connect()
    try:
        ensure_tables(conn, cfg)

        control = read_control(conn, cfg)
        id_inicial = control.get("id_inicial")
        id_final = control.get("id_final")
        id_atual_merged = control.get("id_atual_merged")

        if id_inicial is None or id_final is None:
            raise RuntimeError(f"range_scan_control precisa ter id_inicial e id_final preenchidos. Linha: {control}")

        # se id_atual_merged não estiver preenchido, começa do id_inicial
        if id_atual_merged is None:
            id_atual_merged = id_inicial

        id_inicial = int(id_inicial)
        id_final = int(id_final)
        id_atual_merged = int(id_atual_merged)

        # Esperado: id_inicial alto -> id_final baixo (desc)
        if id_inicial < id_final:
            logging.warning("id_inicial < id_final. Mesmo assim vou varrer desc a partir de id_atual_merged.")

        if id_atual_merged < id_final:
            logging.info(
                f"Nada a fazer: id_atual_merged ({id_atual_merged}) já está abaixo de id_final ({id_final})."
            )
            return

        low = max(id_final, id_atual_merged - (cfg.batch_size - 1))
        ids = list(range(id_atual_merged, low - 1, -1))

        logging.info(
            f"scan iniciando | schema={cfg.db_schema} merged_table={cfg.merged_table} "
            f"control_table={cfg.control_table} | range_atual={id_atual_merged}..{low} (desc) "
            f"| batch_size={cfg.batch_size} | rpm={cfg.movi_rpm} | dry_run={cfg.dry_run}"
        )

        client = MovideskClient(cfg.movi_base_url, cfg.movi_token, cfg.movi_rpm, cfg.http_timeout)

        to_upsert: List[Tuple[int, Optional[int], Optional[str], Optional[datetime]]] = []
        checked = 0
        merged_found = 0

        for tid in ids:
            checked += 1
            try:
                payload = client.get_ticket_merged(tid)
            except requests.HTTPError as e:
                logging.warning(f"HTTPError ticket={tid}: {e}")
                continue
            except requests.RequestException as e:
                logging.warning(f"RequestException ticket={tid}: {e}")
                continue

            if not payload:
                # ticket não tem merge (ou 404)
                continue

            row = normalize_merged_payload(payload)
            if not row:
                continue

            merged_found += 1
            to_upsert.append(row)

            # flush periódico (pra não segurar tudo na memória)
            if len(to_upsert) >= 200:
                if cfg.dry_run:
                    logging.info(f"DRY_RUN: iria upsertar {len(to_upsert)} linhas (parcial).")
                else:
                    n = upsert_merged_rows(conn, cfg, to_upsert)
                    logging.info(f"upsert parcial: {n} linhas.")
                to_upsert.clear()

            if checked % 50 == 0:
                logging.info(f"progresso: checked={checked}/{len(ids)} merged_found={merged_found}")

        # flush final
        if to_upsert:
            if cfg.dry_run:
                logging.info(f"DRY_RUN: iria upsertar {len(to_upsert)} linhas (final).")
            else:
                n = upsert_merged_rows(conn, cfg, to_upsert)
                logging.info(f"upsert final: {n} linhas.")

        # Próximo ponteiro: continua do (low - 1)
        next_id = low - 1
        finished = next_id < id_final
        if finished:
            # mantemos apontando no id_final (ou você pode colocar id_final-1)
            next_id = id_final

        if cfg.dry_run:
            logging.info(f"DRY_RUN: não atualizei ponteiro. next_id seria {next_id}, finished={finished}")
        else:
            update_control_pointer(conn, cfg, next_id=next_id, finished=finished)
            logging.info(f"ponteiro atualizado: id_atual_merged={next_id} | finished={finished}")

        logging.info(f"scan concluído: checked={checked}, merged_found={merged_found}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
