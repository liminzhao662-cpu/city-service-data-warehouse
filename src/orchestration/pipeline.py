"""Local stage orchestrator with durable run report and safe completed-stage skips."""
from pathlib import Path
import argparse,json,os,subprocess,sys,time
ROOT=Path(__file__).resolve().parents[2];PY=ROOT/".venv"/"Scripts"/"python.exe"
def valid(path,field,value):
    try:return json.loads(path.read_text(encoding="utf-8")).get(field)==value
    except Exception:return False
def run(name,cmd,skip=False):
    started=time.perf_counter()
    if skip:return {"stage":name,"status":"SKIPPED_COMPLETE","seconds":0}
    subprocess.run(cmd,cwd=ROOT,check=True);return {"stage":name,"status":"SUCCEEDED","seconds":round(time.perf_counter()-started,3)}
def main():
    p=argparse.ArgumentParser();p.add_argument("--force",action="store_true");a=p.parse_args();steps=[]
    steps.append(run("source_validation",[str(PY),str(ROOT/"src"/"ingest"/"validate_year.py")],not a.force and valid(ROOT/"reports"/"ingest"/"validation_2025.json","rows",3655040)))
    transform_cmd=["powershell","-ExecutionPolicy","Bypass","-File",str(ROOT/"scripts"/"run_spark.ps1")] if os.name=="nt" else [str(PY),str(ROOT/"src"/"transform"/"transform_year.py")]
    steps.append(run("spark_transform",transform_cmd,not a.force and valid(ROOT/"reports"/"spark"/"transform_2025.json","parquet_rows",3655040)))
    if os.name=="nt":steps.append(run("mysql_ready",["powershell","-ExecutionPolicy","Bypass","-File",str(ROOT/"scripts"/"mysql"/"start_mysql.ps1")],False))
    steps.append(run("mysql_load",[str(PY),str(ROOT/"src"/"load"/"load_mysql.py")],False))
    steps.append(run("ads",[str(PY),str(ROOT/"src"/"analytics"/"build_ads.py")],not a.force and valid(ROOT/"reports"/"p4"/"ads_build.json","release_key","2025-baseline-v1")))
    governance_cmd=["powershell","-ExecutionPolicy","Bypass","-File",str(ROOT/"scripts"/"run_governance.ps1")] if os.name=="nt" else [str(PY),str(ROOT/"src"/"governance"/"run_governance.py")]
    steps.append(run("data_quality_and_catalog",governance_cmd,not a.force and valid(ROOT/"reports"/"p7"/"data_quality_run.json","status","PASSED")))
    mining_cmd=["powershell","-ExecutionPolicy","Bypass","-File",str(ROOT/"scripts"/"run_mining.ps1")] if os.name=="nt" else [str(PY),str(ROOT/"src"/"mining"/"train_closure_model.py")]
    steps.append(run("spark_ml",mining_cmd,not a.force and valid(ROOT/"reports"/"p6"/"closure_model.json","algorithm","Spark ML logistic regression")))
    steps.append(run("dashboard",[str(PY),str(ROOT/"src"/"dashboard"/"build_dashboard.py")],False));report={"pipeline":"nyc311_2025_baseline","steps":steps,"succeeded":all(x["status"] in {"SUCCEEDED","SKIPPED_COMPLETE"} for x in steps),"finished_epoch":int(time.time())};(ROOT/"reports"/"p5"/"orchestration_run.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps(report,ensure_ascii=False,indent=2))
if __name__=="__main__":main()
