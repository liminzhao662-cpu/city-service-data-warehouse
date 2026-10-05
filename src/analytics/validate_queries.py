"""Execute the eight documented query classes and retain timings/cardinalities."""
from pathlib import Path
import json,sys,time
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from src.load.load_mysql import connect,env
QUERIES={
"Q1_primary_key":"SELECT unique_key,status,agency FROM ticket_current WHERE unique_key=(SELECT MIN(unique_key) FROM ticket_current)",
"Q2_active_cursor_page":"SELECT unique_key,created_date,complaint_type,status FROM ticket_current WHERE agency='DPR' AND status='In Progress' AND (created_date,unique_key)<('2026-01-01','999999999') ORDER BY created_date DESC,unique_key DESC LIMIT 100",
"Q3_daily_trend":"SELECT created_day,SUM(ticket_count) FROM ads_daily_status WHERE release_id=(SELECT release_id FROM release_registry WHERE is_active=TRUE) GROUP BY created_day ORDER BY created_day",
"Q4_top_complaints":"SELECT complaint_type,ticket_count FROM ads_complaint WHERE release_id=(SELECT release_id FROM release_registry WHERE is_active=TRUE) ORDER BY ticket_count DESC LIMIT 20",
"Q5_closed_duration":"SELECT agency,AVG(TIMESTAMPDIFF(SECOND,created_date,closed_date)) FROM ticket_current WHERE status='Closed' AND closed_date>=created_date GROUP BY agency",
"Q6_active_age":"SELECT agency,COUNT(*),AVG(TIMESTAMPDIFF(HOUR,created_date,'2026-01-01')) FROM ticket_current WHERE status<>'Closed' GROUP BY agency",
"Q7_changed_tickets":"SELECT COUNT(*) FROM (SELECT unique_key FROM ticket_observation GROUP BY unique_key HAVING COUNT(*)>1) versions",
"Q8_agency_borough":"SELECT agency,borough,status,ticket_count FROM ads_agency_status WHERE release_id=(SELECT release_id FROM release_registry WHERE is_active=TRUE) ORDER BY ticket_count DESC"
}
def main():
    cnx=connect(env(ROOT/".env"));q=cnx.cursor();result={}
    for name,sql in QUERIES.items():
        t=time.perf_counter();q.execute(sql);rows=q.fetchall();result[name]={"elapsed_ms":round((time.perf_counter()-t)*1000,3),"result_rows":len(rows),"sample":[str(x) for x in rows[:2]]};print(name,result[name]["elapsed_ms"],len(rows))
    result["passed"]=len(result)==8;(ROOT/"reports"/"p4"/"query_validation.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");cnx.close()
if __name__=="__main__":main()
