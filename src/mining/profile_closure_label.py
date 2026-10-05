"""Profile a leakage-safe closure-time prediction target before model training."""
from pathlib import Path
import json, time
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
DWD = ROOT / "data" / "warehouse" / "release=2025_baseline" / "dwd_ticket"

def main():
    spark = (SparkSession.builder.master("local[*]")
             .appName("service-ticket-label-profile")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.sql.shuffle.partitions", "24").getOrCreate())
    spark.sparkContext.setLogLevel("WARN"); started=time.perf_counter()
    try:
        base=spark.read.parquet(str(DWD)).filter(F.col("created_date") < F.lit("2025-12-01"))
        hours=(F.col("closed_date").cast("long")-F.col("created_date").cast("long"))/3600.0
        data=(base.withColumn("closure_hours",hours)
              .withColumn("label",F.when((F.upper("status")=="CLOSED")&(hours>=0)&(hours<=48),1.0).otherwise(0.0))
              .withColumn("split",F.when(F.col("created_date")<"2025-10-01","train").when(F.col("created_date")<"2025-11-01","validation").otherwise("test")))
        dist=[{"split":r["split"],"rows":r["rows"],"positive":r["positive"],"positive_rate":round(r["positive"]/r["rows"],6)} for r in data.groupBy("split").agg(F.count("*").alias("rows"),F.sum("label").cast("long").alias("positive")).orderBy("split").collect()]
        valid=data.filter((F.upper("status")=="CLOSED")&(F.col("closure_hours")>=0)); qs=valid.approxQuantile("closure_hours",[0.25,0.5,0.75,0.9,0.95],0.001)
        report={"scope":"created before 2025-12-01","target":"closed within 48 hours","features_available_at_creation":["agency","complaint_type","borough","open_data_channel_type","created_hour","day_of_week","month"],"split_distribution":dist,"valid_closed_duration_quantiles_hours":dict(zip(["p25","p50","p75","p90","p95"],[round(x,3) for x in qs])),"elapsed_seconds":round(time.perf_counter()-started,3)}
        (ROOT/"reports"/"p6"/"closure_label_profile.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8"); print(json.dumps(report,ensure_ascii=False,indent=2))
    finally:spark.stop()
if __name__=="__main__":main()
