"""Profile saved JSON evidence without changing or imputing source values."""
import collections
import datetime as dt
import json
import pathlib
import statistics

ROOT=pathlib.Path(__file__).resolve().parent

def read_latest(name):
    path=sorted((ROOT/'evidence').glob('*/'+name+'.json'))[-1]
    return path,json.loads(path.read_text(encoding='utf-8'))

def percentile(values,p):
    if not values: return None
    values=sorted(values)
    pos=(len(values)-1)*p
    lo=int(pos)
    hi=min(lo+1,len(values)-1)
    return values[lo]+(values[hi]-values[lo])*(pos-lo)

def profile(rows):
    issues=collections.defaultdict(list)
    durations=[]
    seen=set()
    for row in rows:
        key=row.get('unique_key')
        if not key: issues['missing_key'].append(key)
        if key in seen: issues['duplicate_key'].append(key)
        seen.add(key)
        closed=row.get('closed_date')
        if row.get('status')=='Closed' and not closed: issues['closed_missing_date'].append(key)
        if row.get('status')!='Closed' and closed: issues['nonclosed_with_close_date'].append(key)
        if closed:
            try:
                hours=(dt.datetime.fromisoformat(closed)-dt.datetime.fromisoformat(row['created_date'])).total_seconds()/3600
                if hours<0: issues['negative_duration'].append(key)
                elif row.get('status')=='Closed': durations.append(hours)
            except (ValueError,KeyError): issues['invalid_timestamp'].append(key)
    return {
        'rows':len(rows),'unique_keys':len(seen),
        'min_created':min(r['created_date'] for r in rows),
        'max_created':max(r['created_date'] for r in rows),
        'status_counts':dict(collections.Counter(r.get('status','MISSING') for r in rows)),
        'agency_counts':dict(collections.Counter(r.get('agency','MISSING') for r in rows)),
        'missing_counts':{field:sum(not r.get(field) for r in rows) for field in ('closed_date','due_date','resolution_action_updated_date')},
        'system_updated_field_note':'Selected for cohort_rows only; absent in other samples by query design.',
        'issues':{k:{'count':len(v),'example_keys':v[:10]} for k,v in issues.items()},
        'valid_closed_durations':len(durations),
        'closed_duration_median_hours':statistics.median(durations) if durations else None,
        'closed_duration_p90_hours':percentile(durations,.9),
        'source_updated_distinct':len({r.get(':updated_at') for r in rows if r.get(':updated_at')}),
        'source_updated_top':collections.Counter(r.get(':updated_at') for r in rows if r.get(':updated_at')).most_common(5),
    }

if __name__=='__main__':
    result={'scope_note':'Chronological samples are not random. Cohort is only BRONX requests created 2026-09-14; values reflect observation time, not historical daily state. Durations are naive source wall-clock differences pending timezone policy.'}
    for name in ('recent_sample','historical_sample','cohort_rows'):
        path,rows=read_latest(name)
        result[name]={'source_file':str(path.relative_to(ROOT)),**profile(rows)}
    _,count=read_latest('cohort_count')
    result['cohort_count_check']={'api_count':int(count[0]['n']),'downloaded':result['cohort_rows']['rows'],'matches':int(count[0]['n'])==result['cohort_rows']['rows']}
    _,updated=read_latest('updated_old_rows')
    result['updated_old_examples']=updated[:3]
    _,statuses=read_latest('recent_status')
    result['recent_week_status_counts']=statuses
    (ROOT/'sample_profile.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))
