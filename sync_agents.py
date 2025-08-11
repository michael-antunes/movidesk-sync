import os
import requests
import psycopg2
from psycopg2.extras import execute_values
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
    """)
    cur.close(); conn.close()

def try_persons(base, expands):
    last_exc=None
    for exp in expands:
        p=base.copy()
        if exp: p['$expand']=exp
        else: p.pop('$expand', None)
        try:
            return api_get('https://api.movidesk.com/public/v1/persons', p)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code==400:
                last_exc=e
                continue
            raise
    if last_exc: raise last_exc
    return []

def list_all_agents():
    top=1000; skip=0; out=[]
    base={'token':MOVIDESK_TOKEN, '$select':'id,businessName,email,isAgent,isActive', '$top':top}
    expands=[
        'teams($select=businessName),profiles($select=businessName)',
        'teams($select=businessName)',
        None
    ]
    while True:
        params=base.copy(); params['$skip']=skip; params['$filter']="isAgent eq true"
        batch=try_persons(params, expands)
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
        if len(batch) < top: break
        skip += len(batch)
    return out

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
    rows=list_all_agents()
    upsert_agents(rows)

if __name__ == '__main__':
    run()
