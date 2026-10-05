"""Run rule-driven data-quality checks and register a lightweight asset catalog."""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import mysql.connector
from pyspark.sql import SparkSession, functions as F


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def connect_mysql(config: dict[str, str]):
    return mysql.connector.connect(
        host=config["MYSQL_HOST"],
        port=int(config["MYSQL_PORT"]),
        user="root",
        password=config["MYSQL_ROOT_PASSWORD"],
        database=config["MYSQL_DATABASE"],
        autocommit=False,
        connection_timeout=30,
    )


def apply_ddl(connection, ddl_path: Path) -> None:
    cursor = connection.cursor()
    for statement in ddl_path.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            cursor.execute(statement)
    connection.commit()
    cursor.close()


def evaluate(rule: dict, value: float, row_count: int) -> tuple[str, float]:
    operator = rule["operator"]
    threshold = float(rule["threshold"])
    if rule["metric"] == "dwd_row_count":
        rate = abs(value - threshold) / threshold if threshold else 0.0
    else:
        rate = value / row_count if row_count else 0.0
    passed = {
        "eq": value == threshold,
        "lte": value <= threshold,
        "rate_lte": rate <= threshold,
    }[operator]
    if passed:
        return "PASS", rate
    return ("FAIL" if rule["severity"] == "BLOCK" else "WARN"), rate


def check_lineage(assets: list[dict], lineage: list[dict]) -> None:
    keys = {asset["asset_key"] for asset in assets}
    graph = {key: [] for key in keys}
    for edge in lineage:
        if edge["upstream"] not in keys or edge["downstream"] not in keys:
            raise ValueError(f"Lineage references unknown asset: {edge}")
        graph[edge["upstream"]].append(edge["downstream"])
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError(f"Lineage cycle detected at {node}")
        if node in visited:
            return
        visiting.add(node)
        for child in graph[node]:
            visit(child)
        visiting.remove(node)
        visited.add(node)

    for asset_key in keys:
        visit(asset_key)


def mysql_table_stats(connection, physical_name: str) -> tuple[int, int]:
    schema, table = physical_name.split(".", 1)
    cursor = connection.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    columns = int(cursor.fetchone()[0])
    cursor.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
    rows = int(cursor.fetchone()[0])
    cursor.close()
    return rows, columns


def register_metadata(connection, config: dict) -> None:
    cursor = connection.cursor()
    asset_sql = """INSERT INTO data_asset(asset_key,asset_name_cn,data_layer,storage_type,physical_name,owner_name,update_cycle,primary_key_desc,partition_desc,description,status)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE asset_name_cn=VALUES(asset_name_cn),data_layer=VALUES(data_layer),storage_type=VALUES(storage_type),physical_name=VALUES(physical_name),owner_name=VALUES(owner_name),update_cycle=VALUES(update_cycle),primary_key_desc=VALUES(primary_key_desc),partition_desc=VALUES(partition_desc),description=VALUES(description),status='ACTIVE'"""
    for asset in config["assets"]:
        cursor.execute(asset_sql, (asset["asset_key"], asset["name_cn"], asset["layer"], asset["storage_type"], asset["physical_name"], asset["owner"], asset["update_cycle"], asset["primary_key"], asset["partitioning"], asset["description"]))
    lineage_sql = """INSERT INTO data_lineage(upstream_asset_key,downstream_asset_key,transform_name) VALUES(%s,%s,%s)
    ON DUPLICATE KEY UPDATE transform_name=VALUES(transform_name)"""
    for edge in config["lineage"]:
        cursor.execute(lineage_sql, (edge["upstream"], edge["downstream"], edge["transform"]))
    rule_sql = """INSERT INTO dq_rule(rule_key,rule_name,quality_dimension,severity,metric_name,comparison_operator,threshold_value,is_enabled)
    VALUES(%s,%s,%s,%s,%s,%s,%s,TRUE) ON DUPLICATE KEY UPDATE rule_name=VALUES(rule_name),quality_dimension=VALUES(quality_dimension),severity=VALUES(severity),metric_name=VALUES(metric_name),comparison_operator=VALUES(comparison_operator),threshold_value=VALUES(threshold_value)"""
    for rule in config["quality_rules"]:
        cursor.execute(rule_sql, (rule["rule_key"], rule["name"], rule["dimension"], rule["severity"], rule["metric"], rule["operator"], rule["threshold"]))
    connection.commit()
    cursor.close()


def load_enabled_quality_rules(connection) -> list[dict]:
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT rule_key, rule_name, quality_dimension, severity, metric_name,
               comparison_operator, threshold_value
        FROM dq_rule
        WHERE is_enabled = TRUE
        ORDER BY rule_key
    """)
    rules = [
        {
            "rule_key": row["rule_key"],
            "name": row["rule_name"],
            "dimension": row["quality_dimension"],
            "severity": row["severity"],
            "metric": row["metric_name"],
            "operator": row["comparison_operator"],
            "threshold": float(row["threshold_value"]),
        }
        for row in cursor.fetchall()
    ]
    cursor.close()
    return rules


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    project = args.project_root.resolve()
    config = json.loads((project / "configs" / "governance.json").read_text(encoding="utf-8"))
    check_lineage(config["assets"], config["lineage"])
    report_dir = project / "reports" / "p7"
    report_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()

    mysql_config = read_env(project / ".env")
    connection = connect_mysql(mysql_config)
    apply_ddl(connection, project / "sql" / "ddl" / "004_governance.sql")
    register_metadata(connection, config)
    enabled_rules = load_enabled_quality_rules(connection)

    dwd_path = project / "data" / "warehouse" / "release=2025_baseline" / "dwd_ticket"
    dws_path = project / "data" / "warehouse" / "release=2025_baseline" / "dws_created_daily"
    spark = (SparkSession.builder.master("local[*]").appName("nyc311-governance")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.sql.shuffle.partitions", "24")
             .config("spark.driver.memory", "4g")
             .config("spark.local.dir", str(project / "data" / "tmp" / "spark"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        dwd = spark.read.parquet(str(dwd_path)).cache()
        aggregates = dwd.agg(
            F.count("*").alias("dwd_row_count"),
            F.countDistinct("unique_key").alias("distinct_key_count"),
            F.sum(F.when(F.col("unique_key").isNull() | (F.trim("unique_key") == ""), 1).otherwise(0)).alias("missing_key"),
            F.sum(F.when(F.col("created_date").isNull() | F.col("agency").isNull() | (F.trim("agency") == "") | F.col("complaint_type").isNull() | (F.trim("complaint_type") == "") | F.col("status").isNull() | (F.trim("status") == ""), 1).otherwise(0)).alias("required_field_null_rows"),
            F.sum(F.when(F.year("created_date") != 2025, 1).otherwise(0)).alias("year_out_of_scope_rows"),
            F.sum(F.when(F.col("latitude").isNull() != F.col("longitude").isNull(), 1).otherwise(0)).alias("coordinate_pair_mismatch_rows"),
            F.sum(F.when((F.col("latitude").isNotNull() & ~F.col("latitude").between(-90, 90)) | (F.col("longitude").isNotNull() & ~F.col("longitude").between(-180, 180)), 1).otherwise(0)).alias("coordinate_out_of_range_rows"),
            F.sum(F.when(F.col("closed_date") < F.col("created_date"), 1).otherwise(0)).alias("negative_duration_rows"),
            F.sum(F.when((F.upper("status") == "CLOSED") & F.col("closed_date").isNull(), 1).otherwise(0)).alias("closed_without_date_rows"),
        ).first().asDict()
        row_count = int(aggregates["dwd_row_count"])
        metrics = {key: int(value or 0) for key, value in aggregates.items()}
        metrics["duplicate_key_rows"] = row_count - metrics.pop("distinct_key_count")
        dws = spark.read.parquet(str(dws_path)).cache()
        dws_record_count = dws.count()
        dws_total = int(dws.agg(F.sum("ticket_count")).first()[0] or 0)
        metrics["dws_record_count"] = dws_record_count
        metrics["dws_ticket_count"] = dws_total
        metrics["dws_reconciliation_gap"] = abs(row_count - dws_total)

        mysql_current_rows, _ = mysql_table_stats(connection, "nyc311.ticket_current")
        metrics["mysql_current_rows"] = mysql_current_rows
        metrics["mysql_reconciliation_gap"] = abs(row_count - mysql_current_rows)

        results = []
        for rule in enabled_rules:
            value = float(metrics[rule["metric"]])
            status, failure_rate = evaluate(rule, value, row_count)
            results.append({**rule, "metric_value": int(value), "failure_rate": round(failure_rate, 10), "status": status})

        blocking_failures = sum(item["status"] == "FAIL" for item in results)
        warnings = sum(item["status"] == "WARN" for item in results)
        passed = sum(item["status"] == "PASS" for item in results)
        overall = "PASSED" if blocking_failures == 0 else "FAILED"
        run_key = f"dq-{config['release_key']}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        cursor = connection.cursor()
        cursor.execute("INSERT INTO dq_run(run_key,release_key,asset_key,status,checked_rows,passed_rules,warning_rules,failed_rules,finished_at) VALUES(%s,%s,'dwd_ticket',%s,%s,%s,%s,%s,CURRENT_TIMESTAMP(3))", (run_key, config["release_key"], overall, row_count, passed, warnings, blocking_failures))
        dq_run_id = cursor.lastrowid
        for item in results:
            message = f"{item['metric']}={item['metric_value']}, operator={item['operator']}, threshold={item['threshold']}"
            cursor.execute("INSERT INTO dq_result(dq_run_id,rule_key,metric_value,failure_rate,result_status,message) VALUES(%s,%s,%s,%s,%s,%s)", (dq_run_id, item["rule_key"], item["metric_value"], item["failure_rate"], item["status"], message))
        connection.commit()
        cursor.close()

        asset_rows = []
        transform_report = json.loads((project / "reports" / "spark" / "transform_2025.json").read_text(encoding="utf-8"))
        raw_report = json.loads((project / "reports" / "ingest" / "validation_2025.json").read_text(encoding="utf-8"))
        for asset in config["assets"]:
            record = dict(asset)
            if asset["storage_type"] == "MySQL":
                rows, columns = mysql_table_stats(connection, asset["physical_name"])
                record.update({"row_count": rows, "column_count": columns, "physical_status": "AVAILABLE"})
            else:
                location = project / asset["physical_name"]
                if asset["asset_key"] == "raw_ticket":
                    rows = int(raw_report["rows"])
                    columns = len(transform_report.get("source_columns", [])) or 20
                elif asset["asset_key"] == "dwd_ticket":
                    rows, columns = row_count, len(dwd.columns)
                else:
                    rows = dws_record_count
                    columns = len(dws.columns)
                record.update({"row_count": rows, "column_count": columns, "physical_status": "AVAILABLE" if location.exists() else "MISSING"})
            asset_rows.append(record)

        report = {
            "release_key": config["release_key"], "dq_run_id": dq_run_id, "run_key": run_key,
            "status": overall, "checked_rows": row_count, "passed_rules": passed,
            "warning_rules": warnings, "failed_rules": blocking_failures, "metrics": metrics,
            "rules": results, "asset_count": len(asset_rows), "lineage_edge_count": len(config["lineage"]),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        (report_dir / "data_quality_run.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        catalog = {"generated_at": datetime.now(timezone.utc).isoformat(), "assets": asset_rows, "lineage": config["lineage"]}
        (report_dir / "data_asset_catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")

        md = ["# P7 数据质量与数据资产报告", "", f"- 质量运行：`{run_key}`", f"- 检查数据量：{row_count:,} 行", f"- 规则结果：{passed} 条通过，{warnings} 条告警，{blocking_failures} 条阻断失败", f"- 资产目录：{len(asset_rows)} 项资产、{len(config['lineage'])} 条血缘关系", f"- 总体状态：**{overall}**", "", "## 质量规则结果", "", "| 规则 | 维度 | 等级 | 指标值 | 失败率 | 结果 |", "|---|---|---:|---:|---:|---|"]
        for item in results:
            md.append(f"| {item['rule_key']} {item['name']} | {item['dimension']} | {item['severity']} | {item['metric_value']:,} | {item['failure_rate']:.6%} | {item['status']} |")
        md += ["", "## 已登记数据资产", "", "| 层级 | 资产 | 存储 | 物理位置 | 行数 | 更新周期 |", "|---|---|---|---|---:|---|"]
        for asset in asset_rows:
            md.append(f"| {asset['layer']} | {asset['name_cn']} | {asset['storage_type']} | `{asset['physical_name']}` | {asset['row_count']:,} | {asset['update_cycle']} |")
        md += ["", "## 实现边界", "", "本模块是单项目范围的数据治理闭环：规则配置、批量检测、结果入库、资产登记和表级血缘。它不等同于企业级元数据平台，也未实现字段级自动血缘。"]
        (report_dir / "P7_数据质量与数据资产报告.md").write_text("\n".join(md) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if overall == "FAILED":
            raise SystemExit(2)
    finally:
        spark.stop()
        connection.close()


if __name__ == "__main__":
    main()
