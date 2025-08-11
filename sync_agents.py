import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from time import sleep

MOVIDESK_TOKEN = os.environ['MOVIDESK_TOKEN']
NEON_DSN = os.environ['NEON_DSN']

def api_get(url, params):
    for i in range(6):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            sleep(min(60, 2**i))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def ensure_structure():
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute("""
        create schema if not exists visualizacao_agentes;
        create table if not exists visualizacao_agentes.agentes(
          agent_id text primary key,
          nome text,
          email text,
          is_agent boolean,
          habilitado boolean,
          permissoes text[],
          equipes text[],
          equipe_principal text,
          imported_at timestamp default now()
        );
        create table if not exists visualizacao_agentes.sync_control(
          name text primary key,
          last_update timestamp not null
        );
    """)
    cur.execute("insert into visualizacao_agentes.sync_control(name,last_update) values('agents_lastupdate', now() at time zone 'utc' - interval '30 days') on conflict (name) do nothing;")
    cur.close(); conn.close()

def get_cursor():
    conn=psycopg2.connect(NEON_DSN); cur=conn.cursor()
    cur.execute("select last_update from visualizacao_agentes.sync_control where name='agents_lastupdate'")
    row=cur.fetchone(); cur.close(); conn.close()
    if not row: return None
    dt=row[0]
    if isinstance(dt,str):
        try: dt=datetime.fromisoformat(dt.replace('Z','+00:00'))
        except Exception: dt=None
    return dt

def set_cursor(ts):
    if ts is None: return
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    cur.execute("update visualizacao_agentes.sync_control set last_update=%s where name='agents_lastupdate'", (ts,))
    cur.close(); conn.close()

def list_agents(cursor_dt):
    top=1000; skip=0; max_lu=cursor_dt; out=[]
    base={'token':MOVIDESK_TOKEN, '$select':'id,businessName,email,isAgent,isActive,lastUpdate', '$expand':'teams($select=businessName),profiles($select=businessName)', '$top':top}
    while True:
        params=base.copy()
        params['$skip']=skip
        if cursor_dt:
            params['$filter']=f"isAgent eq true and lastUpdate ge {cursor_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        else:
            params['$filter']="isAgent eq true"
        batch=api_get('https://api.movidesk.com/public/v1/persons', params)
        if not batch: break
        for p in batch:
            aid=str(p.get('id') or '')
            if not aid: continue
            nome=p.get('businessName') or ''
            email=p.get('email') or ''
            is_agent=bool(p.get('isAgent', False))
            habilitado=bool(p.get('isActive', True))
            equipes=[]
            t=p.get('teams') or []
            if isinstance(t, list):
                for ti in t:
                    if isinstance(ti, dict) and ti.get('businessName'): equipes.append(ti['businessName'])
                    elif isinstance(ti, str): equipes.append(ti)
            permissoes=[]
            prof=p.get('profiles') or []
            if isinstance(prof, list):
                for pr in prof:
                    if isinstance(pr, dict) and pr.get('businessName'): permissoes.append(pr['businessName'])
                    elif isinstance(pr, str): permissoes.append(pr)
            equipe_principal=equipes[0] if equipes else ''
            out.append((aid, nome, email, is_agent, habilitado, permissoes, equipes, equipe_principal))
            lu=p.get('lastUpdate')
            if lu:
                try:
                    dt=datetime.fromisoformat(lu.replace('Z','+00:00'))
                    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
                    if max_lu is None or dt>max_lu: max_lu=dt
                except Exception:
                    pass
        if len(batch)<top: break
        skip+=len(batch)
    return out, max_lu

def upsert_agents(rows):
    if not rows: return
    conn=psycopg2.connect(NEON_DSN); conn.autocommit=True; cur=conn.cursor()
    sql="""
        insert into visualizacao_agentes.agentes(agent_id,nome,email,is_agent,habilitado,permissoes,equipes,equipe_principal,imported_at)
        values %s
        on conflict (agent_id) do update set
          nome=excluded.nome,
          email=excluded.email,
          is_agent=excluded.is_agent,
          habilitado=excluded.habilitado,
          permissoes=excluded.permissoes,
          equipes=excluded.equipes,
          equipe_principal=excluded.equipe_principal,
          imported_at=now();
    """
    execute_values(cur, sql, rows)
    cur.close(); conn.close()

def run():
    ensure_structure()
    c=get_cursor()
    rows,max_lu=list_agents(c)
    if not rows and max_lu is None:
        rows,max_lu=list_agents(None)
    if rows:
        upsert_agents(rows)
    if max_lu:
        set_cursor(max_lu)

if __name__ == '__main__':
    run()
