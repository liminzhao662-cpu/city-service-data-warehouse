"""Train and evaluate a leakage-safe Spark ML closure-time classifier."""
from pathlib import Path
import json, math, shutil, time
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

ROOT=Path(__file__).resolve().parents[2]
DWD=ROOT/"data"/"warehouse"/"release=2025_baseline"/"dwd_ticket"
MODEL=ROOT/"data"/"models"/"closure_48h_logistic_v1"
CATS=["agency","complaint_type","borough","open_data_channel_type"]

def prepare(df):
    hours=(F.col("closed_date").cast("long")-F.col("created_date").cast("long"))/3600.0
    hour=F.hour("created_date");dow=F.dayofweek("created_date");month=F.month("created_date")
    return (df.filter(F.col("created_date")<F.lit("2025-12-01"))
            .withColumn("label",F.when((F.upper("status")=="CLOSED")&(hours>=0)&(hours<=48),1.0).otherwise(0.0))
            .withColumn("created_hour",hour.cast("double"))
            .withColumn("hour_sin",F.sin(hour*F.lit(2*math.pi/24))).withColumn("hour_cos",F.cos(hour*F.lit(2*math.pi/24)))
            .withColumn("dow_sin",F.sin(dow*F.lit(2*math.pi/7))).withColumn("dow_cos",F.cos(dow*F.lit(2*math.pi/7)))
            .withColumn("month_sin",F.sin(month*F.lit(2*math.pi/12))).withColumn("month_cos",F.cos(month*F.lit(2*math.pi/12)))
            .fillna({c:"UNKNOWN" for c in CATS}))

def metrics(pred,threshold):
    p=pred.withColumn("score",vector_to_array("probability")[1]).withColumn("chosen",(F.col("score")>=threshold).cast("double"))
    r=p.agg(F.sum(F.when((F.col("chosen")==1)&(F.col("label")==1),1).otherwise(0)).alias("tp"),F.sum(F.when((F.col("chosen")==1)&(F.col("label")==0),1).otherwise(0)).alias("fp"),F.sum(F.when((F.col("chosen")==0)&(F.col("label")==1),1).otherwise(0)).alias("fn"),F.sum(F.when((F.col("chosen")==0)&(F.col("label")==0),1).otherwise(0)).alias("tn")).first();tp,fp,fn,tn=[int(r[x]) for x in ["tp","fp","fn","tn"]];precision=tp/(tp+fp) if tp+fp else 0;recall=tp/(tp+fn) if tp+fn else 0;f1=2*precision*recall/(precision+recall) if precision+recall else 0
    return {"threshold":threshold,"tp":tp,"fp":fp,"fn":fn,"tn":tn,"precision":round(precision,6),"recall":round(recall,6),"f1":round(f1,6),"accuracy":round((tp+tn)/(tp+fp+fn+tn),6)}

def main():
    spark=(SparkSession.builder.master("local[*]").appName("service-ticket-closure-model").config("spark.sql.session.timeZone","America/New_York").config("spark.sql.shuffle.partitions","24").config("spark.driver.memory","6g").getOrCreate());spark.sparkContext.setLogLevel("WARN");started=time.perf_counter()
    try:
        data=prepare(spark.read.parquet(str(DWD)));train=data.filter(F.col("created_date")<"2025-10-01");valid=data.filter((F.col("created_date")>="2025-10-01")&(F.col("created_date")<"2025-11-01"));test=data.filter((F.col("created_date")>="2025-11-01")&(F.col("created_date")<"2025-12-01"))
        indexers=[StringIndexer(inputCol=c,outputCol=c+"_idx",handleInvalid="keep",stringOrderType="frequencyDesc") for c in CATS];encoder=OneHotEncoder(inputCols=[c+"_idx" for c in CATS],outputCols=[c+"_vec" for c in CATS],handleInvalid="keep");numeric=["hour_sin","hour_cos","dow_sin","dow_cos","month_sin","month_cos"];assembler=VectorAssembler(inputCols=[c+"_vec" for c in CATS]+numeric,outputCol="features",handleInvalid="keep");lr=LogisticRegression(featuresCol="features",labelCol="label",maxIter=30,regParam=0.01,elasticNetParam=0.0,standardization=True);pipeline=Pipeline(stages=indexers+[encoder,assembler,lr]);model=pipeline.fit(train)
        valid_pred=model.transform(valid).select("label","probability","rawPrediction").persist(StorageLevel.MEMORY_AND_DISK);test_pred=model.transform(test).select("label","probability","rawPrediction").persist(StorageLevel.MEMORY_AND_DISK)
        thresholds=[0.30,0.40,0.50,0.60,0.70];valid_grid=[metrics(valid_pred,t) for t in thresholds];best=max(valid_grid,key=lambda x:x["f1"]);test_metrics=metrics(test_pred,best["threshold"]);roc=BinaryClassificationEvaluator(labelCol="label",rawPredictionCol="rawPrediction",metricName="areaUnderROC").evaluate(test_pred);pr=BinaryClassificationEvaluator(labelCol="label",rawPredictionCol="rawPrediction",metricName="areaUnderPR").evaluate(test_pred);pos=test_pred.agg(F.avg("label")).first()[0]
        lr_model=model.stages[-1];meta=model.transform(test.limit(1)).schema["features"].metadata.get("ml_attr",{});attrs=[]
        for group in meta.get("attrs",{}).values():attrs.extend(group)
        names={a["idx"]:a.get("name",f"feature_{a['idx']}") for a in attrs};coefs=lr_model.coefficients.toArray().tolist();top=sorted([{"feature":names.get(i,f"feature_{i}"),"coefficient":round(v,6),"absolute":round(abs(v),6)} for i,v in enumerate(coefs)],key=lambda x:x["absolute"],reverse=True)[:20]
        if MODEL.exists():shutil.rmtree(MODEL)
        model.write().overwrite().save(str(MODEL));report={"task":"predict closure within 48 hours at ticket creation","algorithm":"Spark ML logistic regression","data_scope":"2025-01-01 to 2025-11-30","split":{"train":"Jan-Sep","validation":"Oct","test":"Nov"},"features":{"categorical":CATS,"time_encoding":numeric,"leakage_excluded":["status","closed_date","resolution_action_updated_date","source_updated_at"]},"validation_thresholds":valid_grid,"selected_threshold":best["threshold"],"test":{"rows":test_metrics["tp"]+test_metrics["fp"]+test_metrics["fn"]+test_metrics["tn"],"positive_rate":round(pos,6),"majority_baseline_accuracy":round(max(pos,1-pos),6),"baseline_pr_auc":round(pos,6),"roc_auc":round(roc,6),"pr_auc":round(pr,6),**test_metrics},"top_absolute_coefficients":top,"model_path":str(MODEL),"elapsed_seconds":round(time.perf_counter()-started,3),"limitations":["single-machine Spark local mode","association rather than causal effect","public-source categories may drift","December excluded from test to reduce end-of-window censoring"]};(ROOT/"reports"/"p6"/"closure_model.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps(report,ensure_ascii=False,indent=2));valid_pred.unpersist();test_pred.unpersist()
    finally:spark.stop()
if __name__=="__main__":main()
