package com.portfolio.governance.api;

import com.portfolio.governance.api.dto.*;
import com.portfolio.governance.model.*;
import com.portfolio.governance.service.GovernanceService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class GovernanceController {
    private final GovernanceService service;

    public GovernanceController(GovernanceService service) {
        this.service = service;
    }

    @GetMapping("/overview")
    public GovernanceOverview overview() {
        return service.overview();
    }

    @GetMapping("/assets")
    public PageResult<Asset> assets(
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String layer,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return service.assets(keyword, layer, page, size);
    }

    @GetMapping("/assets/{assetKey}/lineage")
    public List<LineageEdge> lineage(@PathVariable String assetKey) {
        return service.lineage(assetKey);
    }

    @GetMapping("/quality-rules")
    public List<QualityRule> qualityRules() {
        return service.qualityRules();
    }

    @PostMapping("/quality-rules")
    @ResponseStatus(HttpStatus.CREATED)
    public QualityRule createQualityRule(@Valid @RequestBody CreateQualityRuleRequest request) {
        return service.createQualityRule(request);
    }

    @PatchMapping("/quality-rules/{ruleKey}/status")
    public QualityRule updateQualityRuleStatus(
            @PathVariable String ruleKey,
            @Valid @RequestBody UpdateRuleStatusRequest request) {
        return service.updateQualityRuleStatus(ruleKey, request.enabled());
    }

    @GetMapping("/quality-runs")
    public PageResult<QualityRun> qualityRuns(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return service.qualityRuns(status, page, size);
    }

    @GetMapping("/quality-runs/{runId}/results")
    public List<QualityResult> qualityResults(@PathVariable long runId) {
        return service.qualityResults(runId);
    }

    @PostMapping("/quality-jobs")
    public QualityJob createQualityJob(@Valid @RequestBody CreateQualityJobRequest request) {
        return service.createQualityJob(request);
    }

    @GetMapping("/quality-jobs/{jobId}")
    public QualityJob qualityJob(@PathVariable long jobId) {
        return service.qualityJob(jobId);
    }

    @PostMapping("/quality-jobs/{jobId}/start")
    public QualityJob startQualityJob(@PathVariable long jobId) {
        return service.startQualityJob(jobId);
    }

    @PostMapping("/quality-jobs/{jobId}/complete")
    public QualityJob completeQualityJob(
            @PathVariable long jobId,
            @Valid @RequestBody CompleteQualityJobRequest request) {
        return service.completeQualityJob(jobId, request.dqRunId());
    }

    @PostMapping("/quality-jobs/{jobId}/fail")
    public QualityJob failQualityJob(
            @PathVariable long jobId,
            @Valid @RequestBody FailQualityJobRequest request) {
        return service.failQualityJob(jobId, request.errorMessage());
    }
}
