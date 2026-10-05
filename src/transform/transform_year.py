"""Spark SQL normalization for a frozen NYC 311 yearly manifest."""
from __future__ import annotations
import argparse, json, shutil, time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

RAW = [":updated_at","unique_key","created_date","closed_date","agency","agency_name","complaint_type","descriptor","location_type","incident_zip","status","due_date","resolution_action_updated_date","community_board","council_district","police_precinct","borough","open_data_channel_type","latitude","longitude"]
BUSINESS = ["source_updated_at","created_date","closed_date","agency","agency_name","complaint_type","descriptor","location_type","incident_zip","status","due_date","resolution_action_updated_date","community_board","council_district","police_precinct","borough","open_data_channel_type","latitude","longitude"]
MYSQL = ["unique_key",*BUSINESS,"content_hash_hex","quality_flags"]

def args():
    p=argparse.ArgumentParser(); p.add_argument("--project-root",type=Path,default=Path(__file__).resolve().parents[2]); p.add_argument("--year",type=int,default=2025); p.add_argument("--output-root",type=Path); p.add_argument("--shuffle-partitions",type=int,default=24); p.add_argument("--sample-files",type=int,default=0); return p.parse_args()

def main():
    a=args(); project=a.project_root.resolve(); raw_root=project/"data"/"raw"/"nyc311"/f"year={a.year}"; out=(a.output_root or project/"data"/"warehouse"/f"release={a.year}_baseline").resolve(); report_dir=project/"reports"/"spark"; report_dir.mkdir(parents=True,exist_ok=True)
    paths=sorted(raw_root.glob("month=*/*.jsonl.gz")); paths=paths[:a.sample_files] if a.sample_files else paths
    if not paths: raise SystemExit(f"No inputs under {raw_root}")
    if out.exists(): shutil.rmtree(out)
    out.mkdir(parents=True); started=time.perf_counter(); schema=T.StructType([T.StructField(x,T.StringType(),True) for x in RAW])
    spark=(SparkSession.builder.master("local[*]").appName(f"nyc311-{a.year}").config("spark.sql.session.timeZone","America/New_York").config("spark.sql.shuffle.partitions",str(a.shuffle_partitions)).config("spark.driver.memory","4g").config("spark.local.dir",str(project/"data"/"tmp"/"spark")).getOrCreate()); spark.sparkContext.setLogLevel("WARN")
    try:
        raw=spark.read.schema(schema).json([str(x) for x in paths]); ts=lambda c:F.to_timestamp(F.col(c),"yyyy-MM-dd'T'HH:mm:ss.SSS")
        n=raw.select(F.trim("unique_key").alias("unique_key"),F.to_timestamp(F.col(":updated_at"),"yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("source_updated_at"),ts("created_date").alias("created_date"),ts("closed_date").alias("closed_date"),F.trim("agency").alias("agency"),F.trim("agency_name").alias("agency_name"),F.trim("complaint_type").alias("complaint_type"),F.trim("descriptor").alias("descriptor"),F.trim("location_type").alias("location_type"),F.trim("incident_zip").alias("incident_zip"),F.trim("status").alias("status"),ts("due_date").alias("due_date"),ts("resolution_action_updated_date").alias("resolution_action_updated_date"),F.trim("community_board").alias("community_board"),F.trim("council_district").alias("council_district"),F.trim("police_precinct").alias("police_precinct"),F.coalesce(F.upper(F.trim("borough")),F.lit("UNKNOWN")).alias("borough"),F.trim("open_data_channel_type").alias("open_data_channel_type"),F.col("latitude").cast(T.DecimalType(10,7)).alias("latitude"),F.col("longitude").cast(T.DecimalType(10,7)).alias("longitude"))
        flags=F.array_compact(F.array(F.when(F.col("unique_key").isNull()|(F.col("unique_key")==""),"MISSING_KEY"),F.when(F.col("created_date").isNull(),"INVALID_CREATED_DATE"),F.when(F.col("agency").isNull()|(F.col("agency")==""),"MISSING_AGENCY"),F.when(F.col("complaint_type").isNull()|(F.col("complaint_type")==""),"MISSING_COMPLAINT"),F.when(F.col("status").isNull()|(F.col("status")==""),"MISSING_STATUS"),F.when(F.col("closed_date")<F.col("created_date"),"NEGATIVE_DURATION"),F.when((F.upper("status")=="CLOSED")&F.col("closed_date").isNull(),"CLOSED_WITHOUT_DATE"),F.when((F.upper("status")!="CLOSED")&F.col("closed_date").isNotNull(),"ACTIVE_WITH_CLOSED_DATE")))
        canonical=F.concat_ws("\u001f",*[F.coalesce(F.col(c).cast("string"),F.lit("<NULL>")) for c in BUSINESS])
        e=n.withColumn("quality_flags",F.to_json(flags)).withColumn("content_hash_hex",F.sha2(canonical,256)).withColumn("created_year",F.year("created_date")).withColumn("created_month",F.month("created_date"))
        reject_expr=F.col("unique_key").isNull()|(F.col("unique_key")=="")|F.col("created_date").isNull()|F.col("agency").isNull()|(F.col("agency")=="")|F.col("complaint_type").isNull()|(F.col("complaint_type")=="")|F.col("status").isNull()|(F.col("status")=="")
        rejects=e.filter(reject_expr); valid=e.filter(~reject_expr); w=Window.partitionBy("unique_key").orderBy(F.col("source_updated_at").desc_nulls_last(),F.col("content_hash_hex").desc()); current=valid.withColumn("_rn",F.row_number().over(w)).filter("_rn=1").drop("_rn").cache()
        source_rows=raw.count(); rejected=rejects.count(); rows=current.count(); distinct=current.select("unique_key").distinct().count(); arr=T.ArrayType(T.StringType()); flagged=current.filter(F.size(F.from_json("quality_flags",arr))>0).count()
        dwd=out/"dwd_ticket"; current.write.mode("overwrite").partitionBy("created_year","created_month").parquet(str(dwd))
        dws=current.groupBy(F.to_date("created_date").alias("created_day"),"agency","borough","status").agg(F.count("*").alias("ticket_count")); dws_path=out/"dws_created_daily"; dws.write.mode("overwrite").partitionBy("agency").parquet(str(dws_path)); dws_rows=dws.count()
        # MySQL receives timezone-free wall-clock strings. Spark has already converted
        # source UTC instants into the configured America/New_York session timezone.
        export_df=current.select(*[
            F.date_format(c,"yyyy-MM-dd HH:mm:ss.SSS").alias(c) if c in {"source_updated_at","created_date","closed_date","due_date","resolution_action_updated_date"} else F.col(c)
            for c in MYSQL
        ])
        export=out/"mysql_export"; export_df.repartition(a.shuffle_partitions).write.mode("overwrite").option("header","false").option("delimiter","\t").option("quote",'"').option("escape","\\").option("nullValue","\\N").csv(str(export))
        parquet_rows=spark.read.parquet(str(dwd)).count(); export_rows=spark.read.option("delimiter","\t").csv(str(export)).count(); months={f"{int(r['created_month']):02d}":r['count'] for r in current.groupBy("created_month").count().orderBy("created_month").collect()}; fc={r['flag']:r['count'] for r in current.select(F.explode(F.from_json("quality_flags",arr)).alias("flag")).groupBy("flag").count().collect()}
        report={"year":a.year,"sample_files":a.sample_files,"input_file_count":len(paths),"source_rows":source_rows,"rejected_rows":rejected,"current_rows":rows,"distinct_keys":distinct,"flagged_rows":flagged,"quality_flag_counts":fc,"month_counts":months,"parquet_rows":parquet_rows,"mysql_export_rows":export_rows,"dws_rows":dws_rows,"elapsed_seconds":round(time.perf_counter()-started,3),"spark_version":spark.version,"shuffle_partitions":a.shuffle_partitions,"output_root":str(out)}
        suffix=f"sample_{a.sample_files}" if a.sample_files else str(a.year); (report_dir/f"transform_{suffix}.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8"); print(json.dumps(report,ensure_ascii=False,indent=2))
    finally: spark.stop()
if __name__=="__main__": main()
