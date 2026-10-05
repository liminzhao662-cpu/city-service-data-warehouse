package com.portfolio.governance.repository;

import com.portfolio.governance.model.Asset;
import com.portfolio.governance.model.QualityJob;
import com.portfolio.governance.model.QualityRule;
import com.portfolio.governance.model.LineageEdge;
import com.portfolio.governance.model.QualityResult;
import com.portfolio.governance.model.QualityRun;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class GovernanceRepository {
    private final JdbcClient jdbc;

    public GovernanceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public long countAssets(String keyword, String layer) {
        return jdbc.sql("""
                SELECT COUNT(*) FROM data_asset
                WHERE status='ACTIVE'
                  AND (:keyword='' OR asset_key LIKE CONCAT('%',:keyword,'%') OR asset_name_cn LIKE CONCAT('%',:keyword,'%'))
                  AND (:layer='' OR data_layer=:layer)
                """).param("keyword", keyword).param("layer", layer).query(Long.class).single();
    }

    public List<Asset> findAssets(String keyword, String layer, int limit, long offset) {
        return jdbc.sql("""
                SELECT asset_key,asset_name_cn,data_layer,storage_type,physical_name,owner_name,
                       update_cycle,primary_key_desc,partition_desc,description,status,updated_at
                FROM data_asset
                WHERE status='ACTIVE'
                  AND (:keyword='' OR asset_key LIKE CONCAT('%',:keyword,'%') OR asset_name_cn LIKE CONCAT('%',:keyword,'%'))
                  AND (:layer='' OR data_layer=:layer)
                ORDER BY FIELD(data_layer,'原始层','明细层','汇总层','服务层','应用层'),asset_key
                LIMIT :limit OFFSET :offset
                """).param("keyword", keyword).param("layer", layer)
                .param("limit", limit).param("offset", offset).query(this::mapAsset).list();
    }

    public Optional<Asset> findAsset(String assetKey) {
        return jdbc.sql("""
                SELECT asset_key,asset_name_cn,data_layer,storage_type,physical_name,owner_name,
                       update_cycle,primary_key_desc,partition_desc,description,status,updated_at
                FROM data_asset WHERE asset_key=:assetKey AND status='ACTIVE'
                """).param("assetKey", assetKey).query(this::mapAsset).optional();
    }

    public List<LineageEdge> findLineage(String assetKey) {
        return jdbc.sql("""
                SELECT 'UPSTREAM' direction,a.asset_key,a.asset_name_cn,a.data_layer,l.transform_name
                FROM data_lineage l JOIN data_asset a ON a.asset_key=l.upstream_asset_key
                WHERE l.downstream_asset_key=:assetKey
                UNION ALL
                SELECT 'DOWNSTREAM',a.asset_key,a.asset_name_cn,a.data_layer,l.transform_name
                FROM data_lineage l JOIN data_asset a ON a.asset_key=l.downstream_asset_key
                WHERE l.upstream_asset_key=:assetKey
                ORDER BY direction,asset_key
                """).param("assetKey", assetKey).query((rs, rowNum) -> new LineageEdge(
                rs.getString("direction"), rs.getString("asset_key"), rs.getString("asset_name_cn"),
                rs.getString("data_layer"), rs.getString("transform_name"))).list();
    }

    public long countQualityRuns(String status) {
        return jdbc.sql("SELECT COUNT(*) FROM dq_run WHERE (:status='' OR status=:status)")
                .param("status", status).query(Long.class).single();
    }

    public List<QualityRun> findQualityRuns(String status, int limit, long offset) {
        return jdbc.sql("""
                SELECT dq_run_id,run_key,release_key,asset_key,status,checked_rows,passed_rules,
                       warning_rules,failed_rules,started_at,finished_at
                FROM dq_run WHERE (:status='' OR status=:status)
                ORDER BY dq_run_id DESC LIMIT :limit OFFSET :offset
                """).param("status", status).param("limit", limit).param("offset", offset)
                .query(this::mapQualityRun).list();
    }

    public Optional<QualityRun> findQualityRun(long runId) {
        return jdbc.sql("""
                SELECT dq_run_id,run_key,release_key,asset_key,status,checked_rows,passed_rules,
                       warning_rules,failed_rules,started_at,finished_at
                FROM dq_run WHERE dq_run_id=:runId
                """).param("runId", runId).query(this::mapQualityRun).optional();
    }

    public Optional<QualityRun> findLatestQualityRun() {
        return jdbc.sql("""
                SELECT dq_run_id,run_key,release_key,asset_key,status,checked_rows,passed_rules,
                       warning_rules,failed_rules,started_at,finished_at
                FROM dq_run ORDER BY dq_run_id DESC LIMIT 1
                """).query(this::mapQualityRun).optional();
    }

    public List<QualityResult> findQualityResults(long runId) {
        return jdbc.sql("""
                SELECT r.rule_key,r.rule_name,r.quality_dimension,r.severity,
                       x.metric_value,x.failure_rate,x.result_status,x.message
                FROM dq_result x JOIN dq_rule r ON r.rule_key=x.rule_key
                WHERE x.dq_run_id=:runId
                ORDER BY r.rule_key
                """).param("runId", runId).query((rs, rowNum) -> new QualityResult(
                rs.getString("rule_key"), rs.getString("rule_name"), rs.getString("quality_dimension"),
                rs.getString("severity"), rs.getBigDecimal("metric_value"), rs.getBigDecimal("failure_rate"),
                rs.getString("result_status"), rs.getString("message"))).list();
    }

    public long countActiveAssets() {
        return jdbc.sql("SELECT COUNT(*) FROM data_asset WHERE status='ACTIVE'").query(Long.class).single();
    }

    public long countLineageEdges() {
        return jdbc.sql("SELECT COUNT(*) FROM data_lineage").query(Long.class).single();
    }

    public long countEnabledRules() {
        return jdbc.sql("SELECT COUNT(*) FROM dq_rule WHERE is_enabled=TRUE").query(Long.class).single();
    }


    public List<QualityRule> findQualityRules() {
        return jdbc.sql("""
                SELECT rule_key,rule_name,quality_dimension,severity,metric_name,
                       comparison_operator,threshold_value,is_enabled,updated_at
                FROM dq_rule ORDER BY rule_key
                """).query(this::mapQualityRule).list();
    }

    public Optional<QualityRule> findQualityRule(String ruleKey) {
        return jdbc.sql("""
                SELECT rule_key,rule_name,quality_dimension,severity,metric_name,
                       comparison_operator,threshold_value,is_enabled,updated_at
                FROM dq_rule WHERE rule_key=:ruleKey
                """).param("ruleKey", ruleKey).query(this::mapQualityRule).optional();
    }

    public int insertQualityRule(String ruleKey, String ruleName, String dimension, String severity,
                                 String metricName, String operator, java.math.BigDecimal threshold) {
        return jdbc.sql("""
                INSERT INTO dq_rule(rule_key,rule_name,quality_dimension,severity,metric_name,
                                    comparison_operator,threshold_value,is_enabled)
                VALUES(:ruleKey,:ruleName,:dimension,:severity,:metricName,:operator,:threshold,TRUE)
                """).param("ruleKey", ruleKey).param("ruleName", ruleName).param("dimension", dimension)
                .param("severity", severity).param("metricName", metricName).param("operator", operator)
                .param("threshold", threshold).update();
    }

    public int updateQualityRuleEnabled(String ruleKey, boolean enabled) {
        return jdbc.sql("UPDATE dq_rule SET is_enabled=:enabled WHERE rule_key=:ruleKey")
                .param("enabled", enabled).param("ruleKey", ruleKey).update();
    }

    public Optional<QualityJob> findQualityJob(long jobId) {
        return jdbc.sql("""
                SELECT job_id,job_key,release_key,asset_key,status,attempt_count,max_retries,
                       requested_by,error_message,dq_run_id,version_no,created_at,started_at,finished_at,updated_at
                FROM dq_job WHERE job_id=:jobId
                """).param("jobId", jobId).query(this::mapQualityJob).optional();
    }

    public Optional<QualityJob> findQualityJobByKey(String jobKey) {
        return jdbc.sql("""
                SELECT job_id,job_key,release_key,asset_key,status,attempt_count,max_retries,
                       requested_by,error_message,dq_run_id,version_no,created_at,started_at,finished_at,updated_at
                FROM dq_job WHERE job_key=:jobKey
                """).param("jobKey", jobKey).query(this::mapQualityJob).optional();
    }

    public int insertQualityJob(String jobKey, String releaseKey, String assetKey, int maxRetries, String requestedBy) {
        return jdbc.sql("""
                INSERT INTO dq_job(job_key,release_key,asset_key,status,max_retries,requested_by)
                VALUES(:jobKey,:releaseKey,:assetKey,'QUEUED',:maxRetries,:requestedBy)
                """).param("jobKey", jobKey).param("releaseKey", releaseKey).param("assetKey", assetKey)
                .param("maxRetries", maxRetries).param("requestedBy", requestedBy).update();
    }

    public int startQualityJob(long jobId, int version) {
        return jdbc.sql("""
                UPDATE dq_job SET status='RUNNING',attempt_count=attempt_count+1,error_message=NULL,
                    started_at=CURRENT_TIMESTAMP(3),finished_at=NULL,version_no=version_no+1
                WHERE job_id=:jobId AND status='QUEUED' AND version_no=:version
                """).param("jobId", jobId).param("version", version).update();
    }

    public int completeQualityJob(long jobId, int version, Long dqRunId) {
        return jdbc.sql("""
                UPDATE dq_job SET status='SUCCEEDED',dq_run_id=:dqRunId,error_message=NULL,
                    finished_at=CURRENT_TIMESTAMP(3),version_no=version_no+1
                WHERE job_id=:jobId AND status='RUNNING' AND version_no=:version
                """).param("dqRunId", dqRunId).param("jobId", jobId).param("version", version).update();
    }

    public int failQualityJob(long jobId, int version, String nextStatus, String errorMessage) {
        return jdbc.sql("""
                UPDATE dq_job SET status=:nextStatus,error_message=:errorMessage,
                    finished_at=CASE WHEN :nextStatus='FAILED' THEN CURRENT_TIMESTAMP(3) ELSE NULL END,
                    version_no=version_no+1
                WHERE job_id=:jobId AND status='RUNNING' AND version_no=:version
                """).param("nextStatus", nextStatus).param("errorMessage", errorMessage)
                .param("jobId", jobId).param("version", version).update();
    }

    private QualityRule mapQualityRule(ResultSet rs, int rowNum) throws SQLException {
        return new QualityRule(rs.getString("rule_key"), rs.getString("rule_name"),
                rs.getString("quality_dimension"), rs.getString("severity"), rs.getString("metric_name"),
                rs.getString("comparison_operator"), rs.getBigDecimal("threshold_value"),
                rs.getBoolean("is_enabled"), instant(rs.getTimestamp("updated_at")));
    }

    private QualityJob mapQualityJob(ResultSet rs, int rowNum) throws SQLException {
        long linkedRun = rs.getLong("dq_run_id");
        Long dqRunId = rs.wasNull() ? null : linkedRun;
        return new QualityJob(rs.getLong("job_id"), rs.getString("job_key"), rs.getString("release_key"),
                rs.getString("asset_key"), rs.getString("status"), rs.getInt("attempt_count"),
                rs.getInt("max_retries"), rs.getString("requested_by"), rs.getString("error_message"),
                dqRunId, rs.getInt("version_no"), instant(rs.getTimestamp("created_at")),
                instant(rs.getTimestamp("started_at")), instant(rs.getTimestamp("finished_at")),
                instant(rs.getTimestamp("updated_at")));
    }

    private Asset mapAsset(ResultSet rs, int rowNum) throws SQLException {
        return new Asset(rs.getString("asset_key"), rs.getString("asset_name_cn"), rs.getString("data_layer"),
                rs.getString("storage_type"), rs.getString("physical_name"), rs.getString("owner_name"),
                rs.getString("update_cycle"), rs.getString("primary_key_desc"), rs.getString("partition_desc"),
                rs.getString("description"), rs.getString("status"), instant(rs.getTimestamp("updated_at")));
    }

    private QualityRun mapQualityRun(ResultSet rs, int rowNum) throws SQLException {
        return new QualityRun(rs.getLong("dq_run_id"), rs.getString("run_key"), rs.getString("release_key"),
                rs.getString("asset_key"), rs.getString("status"), rs.getLong("checked_rows"),
                rs.getInt("passed_rules"), rs.getInt("warning_rules"), rs.getInt("failed_rules"),
                instant(rs.getTimestamp("started_at")), instant(rs.getTimestamp("finished_at")));
    }

    private Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
