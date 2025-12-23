#!/usr/bin/env python3
"""
sync_tickets_merged.py

Sincroniza mesclas de tickets do Movidesk (endpoint /tickets/merged) para Postgres.

Conforme os manuais enviados:
- Endpoint: GET /tickets/merged
- Parâmetros: token (obrigatório), startDate/endDate (YYYY-MM-DD), page (paginação)
- Resposta pode vir em 2 formatos (conforme exemplos):
  1) Lista direta de itens
  2) Objeto com metadados + lista em `mergedTickets`

Itens podem trazer:
- id OU ticketId (ticket "principal")
- mergedTicketsIds (lista de ints OU string "id1;id2;id3")
- mergedTicketsId (int único)
- lastUpdate (ISO com offset OU "YYYY-MM-DD HH:MM:SS")

Compatível com GitHub Actions/Neon:
- Aceita DATABASE_URL **ou** NEON_DSN (e também PGHOST/PGDATABASE/PGUSER/PGPASSWORD).
"""
from __future__ import annotations

import os
import sys
import json
import time
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import psycopg2
import psycopg2.extras


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def getenv_any(names: Sequence[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v
    return default


def parse_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def parse_int(v: Optional[str], default: int) -> int:
    if v is None:
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


def iso_to_dt(value: Optional[str]) -> Optional[datetime]:
    """
    Converte:
      - ISO 8601 (com offset), ex: 2024-05-23T11:08:13.000-03:00
      - ou "YYYY-MM-DD HH:MM:SS"
    """
    if not value:
        return None
    s = value.strip()

    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        pass

    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def dt_to_utc(d: datetime) -> datetime:
    if d.tzinfo is None:
        return d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def safe_schema_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"DB_SCHEMA inválido: {name!r}")
    return name


def chunked(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


@dataclass(frozen=True)
class Config:
    movidesk_base: str
    movidesk_token: str

    db_schema: str
    table_name: str = "tickets_mesclados"

    window_days: int = 7
    http_timeout: int = 60
    http_retries: int = 5
    http_backoff_base: float = 1.5

    full_resync: bool = False
    start_date_override: Optional[date] = None
    end_date_override: Optional[date] = None


def load_config() -> Config:
    base = getenv_any(
        ["MOVIDESK_API_BASE", "MOVIDESK_BASE", "API_BASE_URL"],
        "https://api.movidesk.com/public/v1",
    )
    base = base.rstrip("/")

    token = getenv_any(["MOVIDESK_TOKEN", "MOVIDESK_API_TOKEN", "API_TOKEN", "TOKEN"])
    if not token:
        raise RuntimeError("Falta token do Movidesk. Use MOVIDESK_TOKEN (ou MOVIDESK_API_TOKEN).")

    schema = safe_schema_name(
        getenv_any(["DB_SCHEMA", "PGSCHEMA"], "visualizacao_resolvidos") or "visualizacao_resolvidos"
    )

    window_days = parse_int(os.getenv("MERGED_WINDOW_DAYS"), 7)
    timeout = parse_int(os.getenv("HTTP_TIMEOUT"), 60)
    retries = parse_int(os.getenv("HTTP_RETRIES"), 5)
    backoff = float(getenv_any(["HTTP_BACKOFF_BASE"], "1.5") or "1.5")

    full_resync = parse_bool(os.getenv("FULL_RESYNC"), False)

    start_date_override = None
    end_date_override = None
    sd = getenv_any(["START_DATE", "MOVIDESK_MERGED_START_DATE"])
    ed = getenv_any(["END_DATE", "MOVIDESK_MERGED_END_DATE"])
    if sd:
        start_date_override = datetime.strptime(sd.strip(), "%Y-%m-%d").date()
    if ed:
        end_date_override = datetime.strptime(ed.strip(), "%Y-%m-%d").date()

    return Config(
        movidesk_base=base,
        movidesk_token=token,
        db_schema=schema,
        window_days=max(1, window_days),
        http_timeout=timeout,
        http_retries=max(0, retries),
        http_backoff_base=max(1.0, backoff),
        full_resync=full_resync,
        start_date_override=start_date_override,
        end_date_override=end_date_override,
    )


def pg_connect():
    """
    Aceita:
      - DATABASE_URL
      - NEON_DSN (comum em Neon)
      - PG_DSN / POSTGRES_DSN
      - ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD (e variantes DB_HOST/DB_NAME/...)
    """
    dsn = getenv_any(["DATABASE_URL", "NEON_DSN", "PG_DSN", "POSTGRES_DSN"])
    if dsn:
        return psycopg2.connect(dsn)

    host = getenv_any(["PGHOST", "DB_HOST", "POSTGRES_HOST"])
    db = getenv_any(["PGDATABASE", "DB_NAME", "POSTGRES_DB"])
    user = getenv_any(["PGUSER", "DB_USER", "POSTGRES_USER"])
    pwd = getenv_any(["PGPASSWORD", "DB_PASSWORD", "POSTGRES_PASSWORD"])
    port = getenv_any(["PGPORT", "DB_PORT", "POSTGRES_PORT"], "5432")

    if not (host and db and user and pwd):
        raise RuntimeError(
            "Faltam variáveis de Postgres. Use DATABASE_URL ou NEON_DSN, "
            "ou PGHOST/PGDATABASE/PGUSER/PGPASSWORD (ou DB_HOST/DB_NAME/DB_USER/DB_PASSWORD)."
        )

    return psycopg2.connect(host=host, port=int(port or 5432), dbname=db, user=user, password=pwd)


def ensure_schema_and_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                ticket_id BIGINT NOT NULL,
                merged_ticket_id BIGINT NOT NULL,
                last_update TIMESTAMPTZ NULL,
                raw JSONB NULL,
                PRIMARY KEY (ticket_id, merged_ticket_id)
            );
            """
        )
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS last_update TIMESTAMPTZ NULL;')
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS raw JSONB NULL;')
    conn.commit()


def get_max_last_update(conn, schema: str, table: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        try:
            cur.execute(f'SELECT MAX(last_update) FROM "{schema}"."{table}";')
            row = cur.fetchone()
            if row and row[0]:
                if isinstance(row[0], datetime):
                    return row[0]
                return iso_to_dt(str(row[0]))
        except psycopg2.Error as ex:
            conn.rollback()
            eprint(f"[WARN] Não consegui ler MAX(last_update) de {schema}.{table}: {ex}")
            return None
    return None


def upsert_rows(
    conn,
    schema: str,
    table: str,
    rows: Sequence[Tuple[int, int, Optional[datetime], Dict[str, Any]]],
) -> int:
    if not rows:
        return 0

    values = [(tid, mid, lu, json.dumps(raw, ensure_ascii=False)) for tid, mid, lu, raw in rows]

    sql = f"""
        INSERT INTO "{schema}"."{table}" (ticket_id, merged_ticket_id, last_update, raw)
        VALUES %s
        ON CONFLICT (ticket_id, merged_ticket_id) DO UPDATE
        SET
            last_update = GREATEST(EXCLUDED.last_update, "{table}".last_update),
            raw = EXCLUDED.raw;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    conn.commit()
    return len(rows)


class MovideskClient:
    def __init__(self, base: str, token: str, timeout: int, retries: int, backoff_base: float):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.retries = retries
        self.backoff_base = backoff_base
        self.session = requests.Session()

    def _request(self, path: str, params: Dict[str, Any]) -> Any:
        url = f"{self.base}{path}"
        params = dict(params)
        params["token"] = self.token

        last_err: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)

                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    delay = (self.backoff_base ** attempt) + (0.1 * attempt)
                    eprint(
                        f"[WARN] HTTP {resp.status_code} em {path}. "
                        f"Tentativa {attempt+1}/{self.retries+1}. Esperando {delay:.1f}s"
                    )
                    time.sleep(delay)
                    last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
                    continue

                if resp.status_code >= 400:
                    raise RuntimeError(f"HTTP {resp.status_code} em {path}: {resp.text}")

                return resp.json()
            except Exception as ex:
                last_err = ex
                if attempt < self.retries:
                    delay = (self.backoff_base ** attempt) + (0.1 * attempt)
                    eprint(
                        f"[WARN] erro ao chamar {path}: {ex}. "
                        f"Tentativa {attempt+1}/{self.retries+1}. Esperando {delay:.1f}s"
                    )
                    time.sleep(delay)
                else:
                    break

        raise last_err or RuntimeError("Falha desconhecida ao chamar API")

    @staticmethod
    def _extract_items(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("mergedTickets"), list):
            return [x for x in payload["mergedTickets"] if isinstance(x, dict)]
        return []

    @staticmethod
    def _extract_total_pages(payload: Any) -> Optional[int]:
        if isinstance(payload, dict):
            pn = payload.get("pageNumber")
            if isinstance(pn, str):
                m = re.search(r"of\s+(\d+)", pn)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return None
        return None

    def iter_ticket_merges(self, start_d: date, end_d: date) -> Iterable[Dict[str, Any]]:
        page = 1
        total_pages: Optional[int] = None

        while True:
            payload = self._request(
                "/tickets/merged",
                {
                    "startDate": start_d.strftime("%Y-%m-%d"),
                    "endDate": end_d.strftime("%Y-%m-%d"),
                    "page": page,
                },
            )

            items = self._extract_items(payload)
            if not items:
                return

            for item in items:
                yield item

            if total_pages is None:
                total_pages = self._extract_total_pages(payload)

            page += 1
            if total_pages is not None and page > total_pages:
                return


def date_range_windows(start_d: date, end_d: date, window_days: int) -> Iterable[Tuple[date, date]]:
    cur = start_d
    while cur <= end_d:
        win_end = min(end_d, cur + timedelta(days=window_days - 1))
        yield cur, win_end
        cur = win_end + timedelta(days=1)


def _parse_merged_ids(value: Any) -> List[int]:
    if isinstance(value, list):
        out: List[int] = []
        for x in value:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out

    if isinstance(value, str):
        parts = [p.strip() for p in value.split(";") if p.strip()]
        out2: List[int] = []
        for p in parts:
            try:
                out2.append(int(p))
            except Exception:
                pass
        return out2

    return []


def normalize_merge_rows(
    items: Iterable[Dict[str, Any]],
    since_dt: Optional[datetime],
) -> List[Tuple[int, int, Optional[datetime], Dict[str, Any]]]:
    best: Dict[Tuple[int, int], Tuple[Optional[datetime], Dict[str, Any]]] = {}
    since_utc = dt_to_utc(since_dt) if since_dt else None

    for it in items:
        tid_raw = it.get("id", None)
        if tid_raw is None:
            tid_raw = it.get("ticketId", None)

        try:
            tid = int(tid_raw)
        except Exception:
            continue

        lu = iso_to_dt(it.get("lastUpdate"))
        lu_utc = dt_to_utc(lu) if lu else None

        if since_utc and lu_utc and lu_utc <= since_utc:
            continue

        merged_ids: List[int] = []
        merged_ids.extend(_parse_merged_ids(it.get("mergedTicketsIds")))
        if it.get("mergedTicketsId") is not None:
            try:
                merged_ids.append(int(it["mergedTicketsId"]))
            except Exception:
                pass

        merged_ids = sorted(set(merged_ids))

        for mid in merged_ids:
            key = (tid, mid)
            prev = best.get(key)
            if prev is None:
                best[key] = (lu_utc, it)
            else:
                prev_lu = prev[0]
                if (prev_lu is None and lu_utc is not None) or (prev_lu and lu_utc and lu_utc > prev_lu):
                    best[key] = (lu_utc, it)

    return [(tid, mid, lu, raw) for (tid, mid), (lu, raw) in best.items()]


def main() -> None:
    cfg = load_config()

    conn = pg_connect()
    try:
        ensure_schema_and_table(conn, cfg.db_schema, cfg.table_name)

        since_dt = None if cfg.full_resync else get_max_last_update(conn, cfg.db_schema, cfg.table_name)

        today_utc = datetime.now(timezone.utc).date()
        if cfg.start_date_override:
            start_d = cfg.start_date_override
        else:
            start_d = (dt_to_utc(since_dt) - timedelta(days=1)).date() if since_dt else (today_utc - timedelta(days=30))

        end_d = cfg.end_date_override or today_utc

        if start_d > end_d:
            start_d, end_d = end_d, start_d

        print(
            f"tickets_mesclados: sync iniciando | schema={cfg.db_schema} tabela={cfg.table_name} | "
            f"janela={start_d}..{end_d} (window_days={cfg.window_days}) | "
            f"since_dt={since_dt.isoformat() if since_dt else 'None'}"
        )

        client = MovideskClient(
            base=cfg.movidesk_base,
            token=cfg.movidesk_token,
            timeout=cfg.http_timeout,
            retries=cfg.http_retries,
            backoff_base=cfg.http_backoff_base,
        )

        total_items = 0
        total_rel = 0

        for w_start, w_end in date_range_windows(start_d, end_d, cfg.window_days):
            print(f"tickets_mesclados: buscando /tickets/merged startDate={w_start} endDate={w_end}")
            items = list(client.iter_ticket_merges(w_start, w_end))
            total_items += len(items)

            rows = normalize_merge_rows(items, since_dt)
            if rows:
                for batch in chunked(rows, 5000):
                    total_rel += upsert_rows(conn, cfg.db_schema, cfg.table_name, batch)
                print(f"tickets_mesclados: janela {w_start}..{w_end}: {len(rows)} relações (upsert).")
            else:
                print(f"tickets_mesclados: janela {w_start}..{w_end}: 0 relações novas.")

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{cfg.db_schema}"."{cfg.table_name}";')
            total = cur.fetchone()[0]

        print(f"tickets_mesclados: concluído. itens_api={total_items} relacoes_upsert={total_rel} total_na_tabela={total}.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
