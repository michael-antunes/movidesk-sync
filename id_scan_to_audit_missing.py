import os
import time
import json
import logging
import urllib.request
import urllib.error
import urllib.parse

import psycopg2
from psycopg2.extras import execute_values


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s  %(message)s")

DSN = os.getenv("NEON_DSN")
TOKEN = os.getenv("MOVIDESK_TOKEN") or os.getenv("MOVIDESK_API_TOKEN")

API_BASE = "https://api.movidesk.com/public/v1"

BATCH_SIZE = int(os.getenv("ID_SCAN_BATCH_SIZE", "50"))
ITERATIONS = int(os.getenv("ID_SCAN_ITERATIONS", "50"))
TABLE_NAME = os.getenv("TABLE_NAME", "tickets_resolvidos")

END_ID = int(os.getenv("ID_SCAN_END_ID", "1"))
START_ID_ENV = os.getenv("ID_SCAN_START_ID")

ATTEMPTS = int(os.getenv("MOVIDESK_ATTEMPTS", "5"))
THROTTLE = float(os.getenv("MOVIDESK_THROTTLE", "0.2"))
TIMEOUT = int(os.getenv("MOVIDESK_TIMEOUT", "60"))

BASE_STATUSES = {"Resolved", "Closed", "Canceled"}
SELECT_MIN = "id,baseStatus,isDeleted,lastUpdate"


if not DSN:
    raise RuntimeError("NEON_DSN não definido")
if not TOKEN:
    raise RuntimeError("MOVIDESK_TOKEN não definido")


def ensure_tables(cur):
    cur.execute("create schema if not exists visualizacao_resolvidos")

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_run(
          id bigserial primary key,
          started_at timestamptz not null default now(),
          window_start timestamptz not null default now(),
          window_end timestamptz not null default now(),
          total_api int not null default 0,
          missing_total int not null default 0,
          total_local int,
          run_at timestamptz,
          notes text
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists started_at timestamptz not null default now()")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_start timestamptz not null default now()")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists window_end timestamptz not null default now()")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists total_api int not null default 0")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists missing_total int not null default 0")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists total_local int")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists run_at timestamptz")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_run add column if not exists notes text")

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.audit_recent_missing(
          run_id bigint references visualizacao_resolvidos.audit_recent_run(id) on delete cascade,
          table_name text not null,
          ticket_id integer not null
        )
        """
    )
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists run_id bigint")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists table_name text")
    cur.execute("alter table visualizacao_resolvidos.audit_recent_missing add column if not exists ticket_id integer")

    cur.execute(
        """
        create unique index if not exists audit_recent_missing_uniq
        on visualizacao_resolvidos.audit_recent_missing(table_name, ticket_id)
        """
    )

    cur.execute(
        """
        create table if not exists visualizacao_resolvidos.id_scan_api_control(
          id_cursor bigint not null
        )
        """
    )
    cur.execute("select count(*) from visualizacao_resolvidos.id_scan_api_control")
    if cur.fetchone()[0] == 0:
        cur.execute("insert into visualizacao_resolvidos.id_scan_api_control(id_cursor) values (0)")


def create_run(cur, notes):
    cur.execute(
        """
        insert into visualizacao_res
