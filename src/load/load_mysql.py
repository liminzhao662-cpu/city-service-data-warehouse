"""Resumable chunked load from Spark export into MySQL current/observation tables."""
from __future__ import annotations
import argparse, hashlib, json, shutil, time
from pathlib import Path
import mysql.connector

COLS=["unique_key","source_updated_at","created_date","closed_date","agency","agency_name","complaint_type","descriptor","location_type","incident_zip","status","due_date","resolution_action_updated_date","community_board","council_district","police_precinct","borough","open_data_channel_type","latitude","longitude","content_hash_hex","quality_flags"]
DATA_COLS=[x for x in COLS if x not in {"content_hash_hex"}]
TS={"source_updated_at","created_date","closed_date","due_date","resolution_action_updated_date"}

def env(path):
    result={}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            k,v=line.split("=",1); result[k.strip()]=v.strip()
    return result

def connect(cfg):
    return mysql.connector.connect(host=cfg["MYSQL_HOST"],port=int(cfg["MYSQL_PORT"]),user="root",password=cfg["MYSQL_ROOT_PASSWORD"],database=cfg["MYSQL_DATABASE"],autocommit=False,connection_timeout=30)

def digest(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def load_sql(server_path,run_id):
    variables=",".join("@"+x for x in COLS)
    sets=[f"run_id={run_id}"]
    for c in DATA_COLS:
        if c in TS: sets.append(f"{c}=STR_TO_DATE(REPLACE(LEFT(NULLIF(@{c},'\\\\N'),23),'T',' '),'%Y-%m-%d %H:%i:%s.%f')")
        elif c in {"latitude","longitude"}: sets.append(f"{c}=NULLIF(@{c},'\\\\N')")
        else: sets.append(f"{c}=NULLIF(@{c},'\\\\N')")
    sets += ["content_hash=UNHEX(@content_hash_hex)","quality_flags=NULLIF(@quality_flags,'\\\\N')"]
    p=server_path.replace("\\","/").replace("'","''")
    return f"LOAD DATA INFILE '{p}' INTO TABLE staging_ticket CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\r\\n' ({variables}) SET {','.join(sets)}"

OBS="""INSERT IGNORE INTO ticket_observation(unique_key,run_id,content_hash,source_updated_at,status,closed_date,agency,complaint_type,borough)
SELECT unique_key,%s,content_hash,source_updated_at,status,closed_date,agency,complaint_type,borough FROM staging_ticket WHERE run_id=%s"""

CURRENT_COLS=["unique_key","source_updated_at","created_date","closed_date","agency","agency_name","complaint_type","descriptor","location_type","incident_zip","status","due_date","resolution_action_updated_date","community_board","council_district","police_precinct","borough","open_data_channel_type","latitude","longitude","content_hash","quality_flags","first_seen_run_id","last_seen_run_id"]
UPDATABLE=[x for x in CURRENT_COLS if x not in {"unique_key","first_seen_run_id","last_seen_run_id","content_hash"}]
UPSERT=(f"INSERT INTO ticket_current({','.join(CURRENT_COLS)}) SELECT unique_key,source_updated_at,created_date,closed_date,agency,agency_name,complaint_type,descriptor,location_type,incident_zip,status,due_date,resolution_action_updated_date,community_board,council_district,police_precinct,borough,open_data_channel_type,latitude,longitude,content_hash,quality_flags,%s,%s FROM staging_ticket WHERE run_id=%s ON DUPLICATE KEY UPDATE " + ",".join([f"{c}=IF(ticket_current.content_hash<>VALUES(content_hash),VALUES({c}),ticket_current.{c})" for c in UPDATABLE]+["content_hash=VALUES(content_hash)","last_seen_run_id=VALUES(last_seen_run_id)"]))

def apply_ddl(cnx,project):
    cur=cnx.cursor()
    for stmt in (project/"sql"/"ddl"/"002_reliability.sql").read_text(encoding="utf-8").split(";"):
        if stmt.strip(): cur.execute(stmt)
    cnx.commit(); cur.close()

def main():
    p=argparse.ArgumentParser(); p.add_argument("--project-root",type=Path,default=Path(__file__).resolve().parents[2]); p.add_argument("--export-dir",type=Path); p.add_argument("--run-key",default="baseline-2025-v1"); p.add_argument("--release-key",default="2025-baseline-v1"); p.add_argument("--max-chunks",type=int,default=0); a=p.parse_args()
    project=a.project_root.resolve(); export=(a.export_dir or project/"data"/"warehouse"/"release=2025_baseline"/"mysql_export").resolve(); files=sorted(export.glob("part-*.csv"));
    if not files: raise SystemExit(f"No export parts under {export}")
    cfg=env(project/".env"); cnx=connect(cfg); apply_ddl(cnx,project); cur=cnx.cursor(dictionary=True)
    cur.execute("SELECT * FROM pipeline_run WHERE run_key=%s",(a.run_key,)); run=cur.fetchone()
    if run and run["status"]=="SUCCEEDED": print(json.dumps({"run_id":run["run_id"],"status":"ALREADY_SUCCEEDED"})); return
    if run: run_id=run["run_id"]; cur.execute("UPDATE pipeline_run SET status='RUNNING',error_message=NULL WHERE run_id=%s",(run_id,)); cnx.commit()
    else:
        cur.execute("INSERT INTO pipeline_run(run_key,run_type,scope_start,scope_end,status,source_rows) VALUES(%s,'BASELINE','2025-01-01','2026-01-01','RUNNING',3655040)",(a.run_key,)); run_id=cur.lastrowid; cnx.commit()
    import_dir=Path("E:/nyc311_runtime/import"); import_dir.mkdir(parents=True,exist_ok=True); totals={"inserted":0,"updated":0,"unchanged":0,"staged":0}; started=time.perf_counter()
    try:
        selected=files[:a.max_chunks] if a.max_chunks else files
        for i,f in enumerate(selected,1):
            key=f.name; sha=digest(f); cur.execute("SELECT * FROM pipeline_chunk WHERE run_id=%s AND chunk_key=%s AND status='COMMITTED'",(run_id,key)); done=cur.fetchone()
            if done:
                for k,col in [("inserted","inserted_rows"),("updated","updated_rows"),("unchanged","unchanged_rows"),("staged","row_count")]: totals[k]+=done[col]
                print(f"[{i}/{len(selected)}] skip committed {key}"); continue
            target=import_dir/f"run_{run_id}_{key}"; shutil.copy2(f,target); cnx.commit(); cnx.start_transaction(); c=cnx.cursor()
            c.execute("DELETE FROM staging_ticket WHERE run_id=%s",(run_id,)); c.execute(load_sql(str(target),run_id)); c.execute("SELECT COUNT(*) FROM staging_ticket WHERE run_id=%s",(run_id,)); staged=c.fetchone()[0]
            c.execute("SELECT SUM(t.unique_key IS NULL),SUM(t.unique_key IS NOT NULL AND t.content_hash<>s.content_hash),SUM(t.unique_key IS NOT NULL AND t.content_hash=s.content_hash) FROM staging_ticket s LEFT JOIN ticket_current t ON t.unique_key=s.unique_key WHERE s.run_id=%s",(run_id,)); ins,upd,unch=(int(x or 0) for x in c.fetchone())
            c.execute(OBS,(run_id,run_id)); c.execute(UPSERT,(run_id,run_id,run_id)); c.execute("DELETE FROM staging_ticket WHERE run_id=%s",(run_id,)); c.execute("INSERT INTO pipeline_chunk(run_id,chunk_key,input_sha256,row_count,inserted_rows,updated_rows,unchanged_rows,status,committed_at) VALUES(%s,%s,%s,%s,%s,%s,%s,'COMMITTED',CURRENT_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE input_sha256=VALUES(input_sha256),row_count=VALUES(row_count),inserted_rows=VALUES(inserted_rows),updated_rows=VALUES(updated_rows),unchanged_rows=VALUES(unchanged_rows),status='COMMITTED',committed_at=CURRENT_TIMESTAMP(3)",(run_id,key,sha,staged,ins,upd,unch)); cnx.commit(); target.unlink(missing_ok=True)
            totals["staged"]+=staged; totals["inserted"]+=ins; totals["updated"]+=upd; totals["unchanged"]+=unch; print(f"[{i}/{len(selected)}] {key}: staged={staged} inserted={ins} updated={upd} unchanged={unch}")
        if not a.max_chunks:
            c=cnx.cursor(); c.execute("SELECT SUM(row_count),SUM(inserted_rows),SUM(updated_rows),SUM(unchanged_rows) FROM pipeline_chunk WHERE run_id=%s AND status='COMMITTED'",(run_id,)); staged,ins,upd,unch=(int(x or 0) for x in c.fetchone()); c.execute("SELECT COUNT(*) FROM ticket_current"); current=c.fetchone()[0]; c.execute("SELECT COUNT(*) FROM ticket_observation"); observations=c.fetchone()[0]
            if staged!=3655040 or current!=3655040: raise RuntimeError(f"reconciliation failed staged={staged}, current={current}")
            cnx.commit(); cnx.start_transaction(); c.execute("INSERT INTO release_registry(release_key,run_id,status,is_active,row_count) VALUES(%s,%s,'CANDIDATE',FALSE,%s) ON DUPLICATE KEY UPDATE run_id=VALUES(run_id),status='CANDIDATE',is_active=FALSE,row_count=VALUES(row_count),activated_at=NULL",(a.release_key,run_id,current)); c.execute("UPDATE release_registry SET is_active=FALSE,status=IF(status='ACTIVE','CANDIDATE',status) WHERE is_active=TRUE AND release_key<>%s",(a.release_key,)); c.execute("UPDATE release_registry SET is_active=TRUE,status='ACTIVE',activated_at=CURRENT_TIMESTAMP(3) WHERE release_key=%s",(a.release_key,)); c.execute("UPDATE pipeline_run SET status='SUCCEEDED',staged_rows=%s,inserted_rows=%s,updated_rows=%s,unchanged_rows=%s,finished_at=CURRENT_TIMESTAMP(3) WHERE run_id=%s",(staged,ins,upd,unch,run_id)); cnx.commit()
            report={"run_id":run_id,"run_key":a.run_key,"release_key":a.release_key,"source_rows":3655040,"staged_rows":staged,"inserted_rows":ins,"updated_rows":upd,"unchanged_rows":unch,"current_rows":current,"observation_rows":observations,"chunks":len(files),"elapsed_seconds":round(time.perf_counter()-started,3),"status":"SUCCEEDED"}; out=project/"reports"/"p3"/"mysql_load_2025.json"; out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8"); print(json.dumps(report,ensure_ascii=False,indent=2))
    except Exception as ex:
        cnx.rollback(); cur=cnx.cursor(); cur.execute("UPDATE pipeline_run SET status='FAILED',error_message=%s,finished_at=CURRENT_TIMESTAMP(3) WHERE run_id=%s",(str(ex)[:2000],run_id)); cnx.commit(); raise
    finally: cnx.close()
if __name__=="__main__": main()
