"""Read-only, bounded NYC311 source inspection; standard-library only."""
import concurrent.futures
import argparse
import datetime as dt
import hashlib
import json
import pathlib
import time
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent
BASE = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json'
FIELDS = 'unique_key,created_date,closed_date,agency,complaint_type,descriptor,status,borough,resolution_action_updated_date,due_date'

QUERIES = {
    'metadata': ('https://data.cityofnewyork.us/api/views/erm2-nwe9.json', None),
    'extent': (BASE, {'$select':'count(*) as n,min(created_date) as earliest,max(created_date) as latest'}),
    'recent_status': (BASE, {'$select':'status,count(*) as n', '$where':"created_date >= '2026-09-14T00:00:00' AND created_date < '2026-09-21T00:00:00'", '$group':'status'}),
    'recent_sample': (BASE, {'$select':FIELDS, '$where':"created_date >= '2026-09-14T00:00:00' AND created_date < '2026-09-21T00:00:00'", '$order':'created_date,unique_key', '$limit':'5000'}),
    'system_fields': (BASE, {'$select':':id,:created_at,:updated_at,'+FIELDS, '$limit':'10'}),
    'recent_closures_old_requests': (BASE, {'$select':FIELDS, '$where':"created_date < '2026-08-01T00:00:00' AND closed_date >= '2026-09-14T00:00:00' AND closed_date < '2026-09-21T00:00:00'", '$order':'closed_date,unique_key', '$limit':'30'}),
    'historical_sample': (BASE, {'$select':FIELDS, '$where':"created_date >= '2024-01-01T00:00:00' AND created_date < '2024-01-02T00:00:00'", '$order':'created_date,unique_key', '$limit':'1000'}),
}

FOLLOWUP = {
    'cohort_count': (BASE, {'$select':'count(*) as n', '$where':"created_date >= '2026-09-14T00:00:00' AND created_date < '2026-09-15T00:00:00' AND borough = 'BRONX'"}),
    'cohort_rows': (BASE, {'$select':':updated_at,'+FIELDS, '$where':"created_date >= '2026-09-14T00:00:00' AND created_date < '2026-09-15T00:00:00' AND borough = 'BRONX'", '$order':'created_date,unique_key', '$limit':'5000'}),
    'updated_old_rows': (BASE, {'$select':':updated_at,'+FIELDS, '$where':":updated_at >= '2026-09-21T00:00:00Z' AND created_date < '2026-08-01T00:00:00'", '$limit':'20'}),
}

def fetch(item):
    name, (url, params) = item
    if params:
        url += '?' + urllib.parse.urlencode(params)
    begin = time.monotonic()
    result = {'name':name, 'url':url, 'started_at_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'NYC311-personal-research/0.1'})
        with urllib.request.urlopen(req, timeout=50) as response:
            body = response.read()
            result.update(http_status=response.status, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        payload = json.loads(body)
        (out / (name+'.json')).write_bytes(body)
        result['records'] = len(payload) if isinstance(payload,list) else None
        if name in ('extent','recent_status','system_fields','recent_closures_old_requests'):
            result['preview'] = payload[:5]
        if name == 'metadata':
            result['metadata'] = {k:payload.get(k) for k in ('name','rowsUpdatedAt','publicationDate')}
            result['columns'] = [{'name':c['name'],'fieldName':c['fieldName'],'dataTypeName':c['dataTypeName']} for c in payload.get('columns',[]) if not c['fieldName'].startswith(':')]
    except Exception as exc:
        result['error'] = str(exc)
    result['seconds'] = round(time.monotonic()-begin,3)
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--followup', action='store_true')
    args = parser.parse_args()
    out = ROOT / 'evidence' / dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out.mkdir(parents=True,exist_ok=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(fetch,(FOLLOWUP if args.followup else QUERIES).items()))
    (out/'manifest.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'directory':str(out),'results':results},ensure_ascii=False,indent=2))
