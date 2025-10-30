import os, time, random, json, re, sys
from unidecode import unidecode
import pandas as pd
import requests
import psycopg2

API_TOKEN = os.getenv("MOVIDESK_TOKEN","").strip()
DSN = os.getenv("NEON_DSN","").strip()
CSV_ENV = os.getenv("FIELDS_CSV_PATH","").strip()

def get_with_retry(session, url, params):
    a = 0
    while True:
        r = session.get(url, params=params, timeout=60)
        if r.status_code in (429,500,502,503,504):
            a += 1
            time.sleep(min(60,(2**a)+random.random()))
            continue
        r.raise_for_status()
        return r

def try_load_person_fields(csv_path):
    if not csv_path or not os.path.isfile(csv_path):
        return {}
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
    df = df.rename(columns={c: c.strip() for c in df.columns})
    df = df[df["Campo para"].str.strip().str.lower().eq("pessoa")]
    df = df[df.get("Ativo","Sim").astype(str).str.strip().str.lower().isin(["sim","true","1","ativo"])]
    df = df[["Id","Nome"]].dropna()
    df["Id"] = df["Id"].astype(int)
    return dict(zip(df["Id"].tolist(), df["Nome"].tolist()))

def sanitize_column(name):
    b = unidecode(str(name)).lower()
    b = re.sub(r"[^a-z0-9_]+","_", b).strip("_")
    return (b if b else "campo")[:60]

def fetch_companies():
    url = "https://api.movidesk.com/public/v1/persons"
    items, skip, top = [], 0, 100
    with requests.Session() as s:
        while True:
            params = {"token": API_TOKEN, "$filter": "personType eq 2", "$expand": "customFieldValues", "$top": top, "$skip": skip}
            r = get_with_retry(s, url, params)
            batch = r.json()
            if not batch:
                break
            items.exte
