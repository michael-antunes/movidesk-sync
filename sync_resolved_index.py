import datetime

def as_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

def get_since_utc(conn):
    overlap_min = int(os.getenv("MOVIDESK_OVERLAP_MIN","120"))
    days_back = int(os.getenv("MOVIDESK_INDEX_DAYS","180"))
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    floor = now_utc - datetime.timedelta(days=days_back)
    with conn.cursor() as cur:
        cur.execute("select max(last_index_run_at) from visualizacao_resolvidos.sync_control")
        last_index = as_utc(cur.fetchone()[0])
        cur.execute("select max(last_update) from visualizacao_resolvidos.detail_control")
        max_detail = as_utc(cur.fetchone()[0])
    cands = []
    if last_index: cands.append(last_index - datetime.timedelta(minutes=overlap_min))
    if max_detail: cands.append(max_detail - datetime.timedelta(minutes=overlap_min))
    since = min(cands) if cands else floor
    if since < floor: since = floor
    return since
