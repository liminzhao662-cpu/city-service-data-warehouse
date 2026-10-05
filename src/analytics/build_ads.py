"""Build release-scoped serving tables from the current facts."""
from pathlib import Path
import json,sys,time
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from src.load.load_mysql import connect,env
def main():
    cnx=connect(env(ROOT/".env"));q=cnx.cursor()
    for stmt in (ROOT/"sql"/"ddl"/"003_analytics.sql").read_text(encoding="utf-8").split(";"):
        if stmt.strip():q.execute(stmt)
    cnx.commit();q.execute("SELECT release_id,release_key FROM release_registry WHERE is_active=TRUE");rid,rkey=q.fetchone();cnx.commit();started=time.perf_counter();cnx.start_transaction()
    for table in ["ads_daily_status","ads_agency_status","ads_complaint"]:q.execute(f"DELETE FROM {table} WHERE release_id=%s",(rid,))
    q.execute("INSERT INTO ads_daily_status SELECT %s,DATE(created_date),status,COUNT(*) FROM ticket_current GROUP BY DATE(created_date),status",(rid,));q.execute("INSERT INTO ads_agency_status SELECT %s,agency,borough,status,COUNT(*) FROM ticket_current GROUP BY agency,borough,status",(rid,));q.execute("INSERT INTO ads_complaint SELECT %s,complaint_type,COUNT(*) FROM ticket_current GROUP BY complaint_type",(rid,));cnx.commit();counts={}
    for table in ["ads_daily_status","ads_agency_status","ads_complaint"]:q.execute(f"SELECT COUNT(*) FROM {table} WHERE release_id=%s",(rid,));counts[table]=q.fetchone()[0]
    report={"release_id":rid,"release_key":rkey,"row_counts":counts,"elapsed_seconds":round(time.perf_counter()-started,3)};(ROOT/"reports"/"p4"/"ads_build.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps(report,ensure_ascii=False,indent=2));cnx.close()
if __name__=="__main__":main()
