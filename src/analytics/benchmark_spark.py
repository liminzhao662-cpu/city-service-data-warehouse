"""Compare a full Parquet scan with creation-month partition pruning."""
from pathlib import Path
import json,statistics,time
from pyspark.sql import SparkSession,functions as F
ROOT=Path(__file__).resolve().parents[2];DWD=ROOT/"data"/"warehouse"/"release=2025_baseline"/"dwd_ticket"
def measure(df,repeats=3):
    runs=[];result=None
    for _ in range(repeats):
        t=time.perf_counter();result=[tuple(r) for r in df.groupBy("status").count().orderBy("status").collect()];runs.append((time.perf_counter()-t)*1000)
    return {"runs_ms":[round(x,3) for x in runs],"median_ms":round(statistics.median(runs),3),"rows":sum(x[1] for x in result),"groups":len(result)},result
def main():
    spark=(SparkSession.builder.master("local[*]").appName("nyc311-p4-partition-benchmark").config("spark.sql.session.timeZone","America/New_York").config("spark.sql.shuffle.partitions","24").getOrCreate());spark.sparkContext.setLogLevel("WARN")
    try:
        base=spark.read.parquet(str(DWD));full,full_result=measure(base);jan_df=base.filter((F.col("created_year")==2025)&(F.col("created_month")==1));jan,jan_result=measure(jan_df)
        all_files=list(DWD.rglob("*.parquet"));jan_files=list((DWD/"created_year=2025"/"created_month=1").glob("*.parquet"));report={"spark_version":spark.version,"full_scan":{**full,"files":len(all_files),"file_bytes":sum(x.stat().st_size for x in all_files)},"january_partition":{**jan,"files":len(jan_files),"file_bytes":sum(x.stat().st_size for x in jan_files),"plan":jan_df.groupBy("status").count()._jdf.queryExecution().executedPlan().toString()},"january_expected_rows":348180,"january_row_count_passed":jan["rows"]==348180}
        (ROOT/"reports"/"p4"/"spark_partition_benchmark.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps(report,ensure_ascii=False,indent=2))
    finally:spark.stop()
if __name__=="__main__":main()
