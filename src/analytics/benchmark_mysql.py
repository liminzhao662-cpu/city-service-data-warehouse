"""Measure index and serving-table alternatives with equality checks."""
from pathlib import Path
import json,statistics,sys,time
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from src.load.load_mysql import connect,env
def timed(q,sql,repeats=3):
    values=[];rows=None
    for _ in range(repeats):
        t=time.perf_counter();q.execute(sql);rows=q.fetchall();values.append((time.perf_counter()-t)*1000)
    return {"runs_ms":[round(x,3) for x in values],"median_ms":round(statistics.median(values),3),"result_rows":len(rows)},rows
def plan(q,sql):q.execute("EXPLAIN ANALYZE "+sql);return "\n".join(x[0] for x in q.fetchall())
def main():
    cnx=connect(env(ROOT/".env"));q=cnx.cursor();q.execute("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='ticket_current' AND index_name='idx_ticket_agency_status_created'");exists=q.fetchone()[0]
    if exists:q.execute("ALTER TABLE ticket_current DROP INDEX idx_ticket_agency_status_created")
    q.execute("CREATE INDEX idx_ticket_agency_status_created ON ticket_current(agency,status,created_date DESC,unique_key DESC) INVISIBLE");cnx.commit();index_sql="SELECT COUNT(*) FROM ticket_current WHERE agency='HPD' AND status='Open' AND created_date>='2025-01-01' AND created_date<'2026-01-01'"
    q.execute("SET SESSION optimizer_switch='use_invisible_indexes=off'");off,off_rows=timed(q,index_sql);off["plan"]=plan(q,index_sql);q.execute("SET SESSION optimizer_switch='use_invisible_indexes=on'");on,on_rows=timed(q,index_sql);on["plan"]=plan(q,index_sql);q.execute("ALTER TABLE ticket_current ALTER INDEX idx_ticket_agency_status_created VISIBLE");cnx.commit()
    detail_sql="SELECT DATE(created_date),status,COUNT(*) FROM ticket_current GROUP BY DATE(created_date),status ORDER BY 1,2";ads_sql="SELECT created_day,status,ticket_count FROM ads_daily_status WHERE release_id=(SELECT release_id FROM release_registry WHERE is_active=TRUE) ORDER BY 1,2";detail,detail_rows=timed(q,detail_sql);ads,ads_rows=timed(q,ads_sql);equal=[tuple(map(str,x)) for x in detail_rows]==[tuple(map(str,x)) for x in ads_rows]
    report={"environment":{"mysql":"8.4.11","rows":3655040,"repeats":3,"cache_note":"same session; all runs retained"},"composite_index":{"query":"HPD/Open full-year count","without_index":off,"with_index":on,"same_result":off_rows==on_rows},"detail_vs_ads":{"detail":detail,"ads":ads,"same_result":equal},"index_definition":"(agency,status,created_date DESC,unique_key DESC)"};(ROOT/"reports"/"p4"/"mysql_benchmark.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps(report,ensure_ascii=False,indent=2));cnx.close()
if __name__=="__main__":main()
