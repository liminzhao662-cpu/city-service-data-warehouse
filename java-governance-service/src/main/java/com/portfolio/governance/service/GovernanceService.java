package com.portfolio.governance.service;

import com.portfolio.governance.api.dto.CreateQualityJobRequest;
import com.portfolio.governance.api.dto.CreateQualityRuleRequest;
import com.portfolio.governance.model.*;
import com.portfolio.governance.repository.GovernanceRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

@Service
@Transactional(readOnly = true)
public class GovernanceService {
    private static final int MAX_PAGE_SIZE = 100;
    private static final Set<String> SUPPORTED_METRICS = Set.of(
            "dwd_row_count", "duplicate_key_rows", "required_field_null_rows",
            "year_out_of_scope_rows", "coordinate_pair_mismatch_rows",
            "coordinate_out_of_range_rows", "negative_duration_rows",
            "closed_without_date_rows", "dws_reconciliation_gap",
            "mysql_reconciliation_gap");
    private final GovernanceRepository repository;

    public GovernanceService(GovernanceRepository repository) {
        this.repository = repository;
    }

    public PageResult<Asset> assets(String keyword, String layer, int page, int size) {
        validatePage(page, size);
        String safeKeyword = normalize(keyword);
        String safeLayer = normalize(layer);
        long total = repository.countAssets(safeKeyword, safeLayer);
        return new PageResult<>(repository.findAssets(safeKeyword, safeLayer, size, (long) page * size), total, page, size);
    }

    public List<LineageEdge> lineage(String assetKey) {
        repository.findAsset(assetKey).orElseThrow(() -> new ResourceNotFoundException("数据资产不存在: " + assetKey));
        return repository.findLineage(assetKey);
    }

    public List<QualityRule> qualityRules() {
        return repository.findQualityRules();
    }

    @Transactional
    public QualityRule createQualityRule(CreateQualityRuleRequest request) {
        String ruleKey = request.ruleKey().trim().toUpperCase();
        String metricName = request.metricName().trim();
        if (!SUPPORTED_METRICS.contains(metricName)) {
            throw new IllegalArgumentException("当前质量作业不支持指标: " + metricName);
        }
        try {
            repository.insertQualityRule(ruleKey, request.ruleName().trim(), request.qualityDimension().trim(),
                    request.severity(), metricName, request.comparisonOperator(), request.thresholdValue());
        } catch (DuplicateKeyException exception) {
            throw new ConflictException("质量规则已存在: " + ruleKey);
        }
        return repository.findQualityRule(ruleKey).orElseThrow();
    }

    @Transactional
    public QualityRule updateQualityRuleStatus(String ruleKey, boolean enabled) {
        String normalizedKey = normalize(ruleKey).toUpperCase();
        repository.findQualityRule(normalizedKey)
                .orElseThrow(() -> new ResourceNotFoundException("质量规则不存在: " + normalizedKey));
        repository.updateQualityRuleEnabled(normalizedKey, enabled);
        return repository.findQualityRule(normalizedKey).orElseThrow();
    }

    public PageResult<QualityRun> qualityRuns(String status, int page, int size) {
        validatePage(page, size);
        String safeStatus = normalize(status).toUpperCase();
        if (!safeStatus.isEmpty() && !List.of("RUNNING", "PASSED", "FAILED").contains(safeStatus)) {
            throw new IllegalArgumentException("status 仅支持 RUNNING、PASSED 或 FAILED");
        }
        long total = repository.countQualityRuns(safeStatus);
        return new PageResult<>(repository.findQualityRuns(safeStatus, size, (long) page * size), total, page, size);
    }

    public List<QualityResult> qualityResults(long runId) {
        repository.findQualityRun(runId).orElseThrow(() -> new ResourceNotFoundException("质量运行不存在: " + runId));
        return repository.findQualityResults(runId);
    }

    @Transactional
    public QualityJob createQualityJob(CreateQualityJobRequest request) {
        String jobKey = request.jobKey().trim();
        String releaseKey = request.releaseKey().trim();
        String assetKey = request.assetKey().trim();
        int maxRetries = request.maxRetries() == null ? 2 : request.maxRetries();
        String requestedBy = normalize(request.requestedBy());
        if (requestedBy.isEmpty()) requestedBy = "manual";

        repository.findAsset(assetKey)
                .orElseThrow(() -> new ResourceNotFoundException("数据资产不存在: " + assetKey));
        var existing = repository.findQualityJobByKey(jobKey);
        if (existing.isPresent()) {
            assertSameJob(existing.get(), releaseKey, assetKey, maxRetries);
            return existing.get();
        }
        try {
            repository.insertQualityJob(jobKey, releaseKey, assetKey, maxRetries, requestedBy);
        } catch (DuplicateKeyException exception) {
            QualityJob concurrent = repository.findQualityJobByKey(jobKey).orElseThrow(() -> exception);
            assertSameJob(concurrent, releaseKey, assetKey, maxRetries);
            return concurrent;
        }
        return repository.findQualityJobByKey(jobKey).orElseThrow();
    }

    public QualityJob qualityJob(long jobId) {
        return getQualityJob(jobId);
    }

    @Transactional
    public QualityJob startQualityJob(long jobId) {
        QualityJob job = getQualityJob(jobId);
        if (!"QUEUED".equals(job.status())) {
            throw new ConflictException("仅QUEUED任务可以启动，当前状态: " + job.status());
        }
        verifyUpdated(repository.startQualityJob(jobId, job.version()), "任务已被其他请求更新");
        return getQualityJob(jobId);
    }

    @Transactional
    public QualityJob completeQualityJob(long jobId, Long dqRunId) {
        QualityJob job = getQualityJob(jobId);
        if (!"RUNNING".equals(job.status())) {
            throw new ConflictException("仅RUNNING任务可以完成，当前状态: " + job.status());
        }
        if (dqRunId != null && repository.findQualityRun(dqRunId).isEmpty()) {
            throw new ResourceNotFoundException("质量运行不存在: " + dqRunId);
        }
        verifyUpdated(repository.completeQualityJob(jobId, job.version(), dqRunId), "任务已被其他请求更新");
        return getQualityJob(jobId);
    }

    @Transactional
    public QualityJob failQualityJob(long jobId, String errorMessage) {
        QualityJob job = getQualityJob(jobId);
        if (!"RUNNING".equals(job.status())) {
            throw new ConflictException("仅RUNNING任务可以登记失败，当前状态: " + job.status());
        }
        String nextStatus = job.attemptCount() <= job.maxRetries() ? "QUEUED" : "FAILED";
        verifyUpdated(repository.failQualityJob(jobId, job.version(), nextStatus, errorMessage.trim()),
                "任务已被其他请求更新");
        return getQualityJob(jobId);
    }

    public GovernanceOverview overview() {
        return new GovernanceOverview(repository.countActiveAssets(), repository.countLineageEdges(),
                repository.countEnabledRules(), repository.findLatestQualityRun().orElse(null));
    }

    private QualityJob getQualityJob(long jobId) {
        return repository.findQualityJob(jobId)
                .orElseThrow(() -> new ResourceNotFoundException("质量任务不存在: " + jobId));
    }

    private void assertSameJob(QualityJob existing, String releaseKey, String assetKey, int maxRetries) {
        if (!existing.releaseKey().equals(releaseKey) || !existing.assetKey().equals(assetKey)
                || existing.maxRetries() != maxRetries) {
            throw new ConflictException("jobKey已被其他任务参数使用: " + existing.jobKey());
        }
    }

    private void verifyUpdated(int updated, String message) {
        if (updated != 1) throw new ConflictException(message);
    }

    private void validatePage(int page, int size) {
        if (page < 0) throw new IllegalArgumentException("page 不能小于0");
        if (size < 1 || size > MAX_PAGE_SIZE) throw new IllegalArgumentException("size 必须在1到100之间");
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim();
    }
}
