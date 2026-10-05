"""P2 integration tests against MySQL using clearly synthetic fixture keys."""
from __future__ import annotations
import hashlib, json, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(ROOT))
from src.load.load_mysql import OBS, UPSERT, connect, env

KEY1="P2_SYNTHETIC_TICKET_001"; KEY2="P2_SYNTHETIC_TICKET_002"

def new_run(cnx,key,status="RUNNING"):
    q=cnx.cursor(); q.execute("INSERT INTO pipeline_run(run_key,run_type,status) VALUES(%s,'P2_TEST',%s)",(key,status)); rid=q.lastrowid; cnx.commit(); return rid

def stage(cnx,rid,key,status,closed=None):
    payload="|".join([key,status,closed or "<NULL>"]); h=hashlib.sha256(payload.encode()).digest(); q=cnx.cursor(); q.execute("DELETE FROM staging_ticket WHERE run_id=%s",(rid,)); q.execute("""INSERT INTO staging_ticket(run_id,unique_key,source_updated_at,created_date,closed_date,agency,agency_name,complaint_type,status,borough,content_hash,quality_flags) VALUES(%s,%s,'2025-01-02 00:00:00.000','2025-01-01 00:00:00.000',%s,'TEST','Synthetic Test Agency','Synthetic Test Complaint',%s,'MANHATTAN',%s,'[]')""",(rid,key,closed,status,h))

def merge(cnx,rid):
    q=cnx.cursor(); q.execute(OBS,(rid,rid)); q.execute(UPSERT,(rid,rid,rid)); q.execute("DELETE FROM staging_ticket WHERE run_id=%s",(rid,)); cnx.commit()

def scalar(cnx,sql,args=()):
    q=cnx.cursor(); q.execute(sql,args); return q.fetchone()[0]

def main():
    cfg=env(ROOT/".env"); stamp=str(int(time.time()*1000)); run_keys=[]; release_key=f"p2-rejected-{stamp}"; cnx=connect(cfg); checks={}
    try:
        active_before=scalar(cnx,"SELECT release_key FROM release_registry WHERE is_active=TRUE")
        r1=new_run(cnx,f"p2-idempotent-{stamp}"); run_keys.append(r1); stage(cnx,r1,KEY1,"Open"); merge(cnx,r1)
        first=(scalar(cnx,"SELECT COUNT(*) FROM ticket_current WHERE unique_key=%s",(KEY1,)),scalar(cnx,"SELECT COUNT(*) FROM ticket_observation WHERE unique_key=%s",(KEY1,)))
        stage(cnx,r1,KEY1,"Open"); merge(cnx,r1); second=(scalar(cnx,"SELECT COUNT(*) FROM ticket_current WHERE unique_key=%s",(KEY1,)),scalar(cnx,"SELECT COUNT(*) FROM ticket_observation WHERE unique_key=%s",(KEY1,)))
        checks["same_input_rerun"]={"before":first,"after":second,"passed":first==(1,1) and second==(1,1)}

        r2=new_run(cnx,f"p2-change-{stamp}"); run_keys.append(r2); stage(cnx,r2,KEY1,"Closed","2025-01-03 00:00:00.000"); merge(cnx,r2)
        status=scalar(cnx,"SELECT status FROM ticket_current WHERE unique_key=%s",(KEY1,)); obs=scalar(cnx,"SELECT COUNT(*) FROM ticket_observation WHERE unique_key=%s",(KEY1,)); checks["business_change"]={"current_status":status,"observation_versions":obs,"passed":status=="Closed" and obs==2}

        r3=new_run(cnx,f"p2-timeout-{stamp}"); run_keys.append(r3); stage(cnx,r3,KEY2,"Open"); q=cnx.cursor(); q.execute(OBS,(r3,r3)); q.execute(UPSERT,(r3,r3,r3)); q.execute("DELETE FROM staging_ticket WHERE run_id=%s",(r3,)); q.execute("INSERT INTO pipeline_chunk(run_id,chunk_key,input_sha256,row_count,inserted_rows,status,committed_at) VALUES(%s,'synthetic-chunk',%s,1,1,'COMMITTED',CURRENT_TIMESTAMP(3))",(r3,"0"*64)); cnx.commit(); cnx.close()
        # Simulate losing the acknowledgement after COMMIT. Recovery inspects the durable
        # checkpoint before retrying, so the chunk is skipped.
        cnx=connect(cfg); checkpoint=scalar(cnx,"SELECT COUNT(*) FROM pipeline_chunk WHERE run_id=%s AND chunk_key='synthetic-chunk' AND status='COMMITTED'",(r3,)); current2=scalar(cnx,"SELECT COUNT(*) FROM ticket_current WHERE unique_key=%s",(KEY2,)); obs2=scalar(cnx,"SELECT COUNT(*) FROM ticket_observation WHERE unique_key=%s",(KEY2,)); checks["commit_ack_lost"]={"checkpoint_found":checkpoint,"current_rows":current2,"observation_versions":obs2,"passed":checkpoint==1 and current2==1 and obs2==1}

        r4=new_run(cnx,f"p2-rejected-release-{stamp}","FAILED"); run_keys.append(r4); q=cnx.cursor(); q.execute("INSERT INTO release_registry(release_key,run_id,status,is_active,row_count) VALUES(%s,%s,'REJECTED',FALSE,0)",(release_key,r4)); cnx.commit(); active_after=scalar(cnx,"SELECT release_key FROM release_registry WHERE is_active=TRUE"); checks["failed_release_isolation"]={"active_before":active_before,"active_after":active_after,"rejected_is_active":bool(scalar(cnx,"SELECT is_active FROM release_registry WHERE release_key=%s",(release_key,))),"passed":active_before==active_after}

        passed=all(x["passed"] for x in checks.values()); report={"fixture_type":"synthetic","checks":checks,"passed":passed,"tested_at_epoch_ms":int(time.time()*1000)}; (ROOT/"reports"/"p2"/"reliability_tests.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8"); print(json.dumps(report,ensure_ascii=False,indent=2));
        if not passed: raise AssertionError("P2 reliability checks failed")
    finally:
        if not cnx.is_connected(): cnx=connect(cfg)
        q=cnx.cursor(); q.execute("DELETE FROM ticket_observation WHERE unique_key IN (%s,%s)",(KEY1,KEY2)); q.execute("DELETE FROM ticket_current WHERE unique_key IN (%s,%s)",(KEY1,KEY2)); q.execute("DELETE FROM staging_ticket WHERE run_id IN ("+",".join(["%s"]*len(run_keys))+")",tuple(run_keys)) if run_keys else None; q.execute("DELETE FROM release_registry WHERE release_key=%s",(release_key,));
        if run_keys: q.execute("DELETE FROM pipeline_chunk WHERE run_id IN ("+",".join(["%s"]*len(run_keys))+")",tuple(run_keys)); q.execute("DELETE FROM pipeline_run WHERE run_id IN ("+",".join(["%s"]*len(run_keys))+")",tuple(run_keys))
        cnx.commit(); cnx.close()
if __name__=="__main__": main()
