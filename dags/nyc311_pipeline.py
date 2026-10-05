"""Airflow deployment adapter; business logic remains in tested project scripts."""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
PROJECT="/opt/nyc311"
with DAG("nyc311_2025_pipeline",start_date=datetime(2025,1,1),schedule=None,catchup=False,max_active_runs=1,tags=["nyc311","data-engineering"]) as dag:
    validate=BashOperator(task_id="validate_source",bash_command=f"cd '{PROJECT}' && .venv/bin/python src/ingest/validate_year.py")
    transform=BashOperator(task_id="spark_transform",bash_command=f"cd '{PROJECT}' && .venv/bin/python src/transform/transform_year.py")
    load=BashOperator(task_id="mysql_load",bash_command=f"cd '{PROJECT}' && .venv/bin/python src/load/load_mysql.py")
    ads=BashOperator(task_id="build_ads",bash_command=f"cd '{PROJECT}' && .venv/bin/python src/analytics/build_ads.py")
    dashboard=BashOperator(task_id="build_dashboard",bash_command=f"cd '{PROJECT}' && .venv/bin/python src/dashboard/build_dashboard.py")
    validate >> transform >> load >> ads >> dashboard
