package com.portfolio.governance.service;

import com.portfolio.governance.api.dto.CreateQualityJobRequest;
import com.portfolio.governance.api.dto.CreateQualityRuleRequest;
import com.portfolio.governance.model.Asset;
import com.portfolio.governance.model.PageResult;
import com.portfolio.governance.model.QualityJob;
import com.portfolio.governance.repository.GovernanceRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GovernanceServiceTest {
    private final GovernanceRepository repository = mock(GovernanceRepository.class);
    private final GovernanceService service = new GovernanceService(repository);

    @Test
    void rejectsUnboundedPageSizeBeforeQueryingDatabase() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> service.assets("", "", 0, 101));
        assertTrue(error.getMessage().contains("1到100"));
        verifyNoInteractions(repository);
    }

    @Test
    void appliesFiltersAndReturnsPageMetadata() {
        when(repository.countAssets("工单", "明细层")).thenReturn(1L);
        when(repository.findAssets("工单", "明细层", 10, 20L)).thenReturn(List.of());
        PageResult<Asset> result = service.assets(" 工单 ", " 明细层 ", 2, 10);
        assertEquals(1L, result.total());
        assertEquals(2, result.page());
        verify(repository).findAssets("工单", "明细层", 10, 20L);
    }

    @Test
    void rejectsUnknownQualityStatus() {
        assertThrows(IllegalArgumentException.class,
                () -> service.qualityRuns("SUCCESS", 0, 20));
        verifyNoInteractions(repository);
    }

    @Test
    void returnsExistingJobForIdempotentSubmission() {
        CreateQualityJobRequest request = new CreateQualityJobRequest(
                "job-2025", "2025-baseline-v1", "dwd_ticket", 2, "scheduler");
        QualityJob existing = job(7L, "QUEUED", 0, 2, 3);
        when(repository.findAsset("dwd_ticket")).thenReturn(Optional.of(mock(Asset.class)));
        when(repository.findQualityJobByKey("job-2025")).thenReturn(Optional.of(existing));

        assertSame(existing, service.createQualityJob(request));
        verify(repository, never()).insertQualityJob(anyString(), anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    void rejectsSameJobKeyWithDifferentParameters() {
        CreateQualityJobRequest request = new CreateQualityJobRequest(
                "job-2025", "another-release", "dwd_ticket", 2, "scheduler");
        when(repository.findAsset("dwd_ticket")).thenReturn(Optional.of(mock(Asset.class)));
        when(repository.findQualityJobByKey("job-2025")).thenReturn(Optional.of(job(7L, "QUEUED", 0, 2, 3)));

        assertThrows(ConflictException.class, () -> service.createQualityJob(request));
    }

    @Test
    void startsQueuedJobWithOptimisticVersion() {
        QualityJob queued = job(7L, "QUEUED", 0, 2, 3);
        QualityJob running = job(7L, "RUNNING", 1, 2, 4);
        when(repository.findQualityJob(7L)).thenReturn(Optional.of(queued), Optional.of(running));
        when(repository.startQualityJob(7L, 3)).thenReturn(1);

        assertEquals("RUNNING", service.startQualityJob(7L).status());
        verify(repository).startQualityJob(7L, 3);
    }

    @Test
    void requeuesFailureWhileRetriesRemain() {
        QualityJob running = job(7L, "RUNNING", 1, 2, 4);
        QualityJob queued = job(7L, "QUEUED", 1, 2, 5);
        when(repository.findQualityJob(7L)).thenReturn(Optional.of(running), Optional.of(queued));
        when(repository.failQualityJob(7L, 4, "QUEUED", "spark timeout")).thenReturn(1);

        assertEquals("QUEUED", service.failQualityJob(7L, "spark timeout").status());
        verify(repository).failQualityJob(7L, 4, "QUEUED", "spark timeout");
    }

    @Test
    void marksFailureTerminalAfterRetryBudgetIsExhausted() {
        QualityJob running = job(7L, "RUNNING", 3, 2, 8);
        QualityJob failed = job(7L, "FAILED", 3, 2, 9);
        when(repository.findQualityJob(7L)).thenReturn(Optional.of(running), Optional.of(failed));
        when(repository.failQualityJob(7L, 8, "FAILED", "rule execution failed")).thenReturn(1);

        assertEquals("FAILED", service.failQualityJob(7L, "rule execution failed").status());
        verify(repository).failQualityJob(7L, 8, "FAILED", "rule execution failed");
    }

    @Test
    void rejectsRuleMetricThatSparkJobCannotCompute() {
        CreateQualityRuleRequest request = new CreateQualityRuleRequest(
                "DQ999", "unsupported", "有效性", "WARN",
                "unknown_metric", "lte", BigDecimal.ZERO);

        assertThrows(IllegalArgumentException.class, () -> service.createQualityRule(request));
        verifyNoInteractions(repository);
    }

    private QualityJob job(long id, String status, int attempts, int retries, int version) {
        return new QualityJob(id, "job-2025", "2025-baseline-v1", "dwd_ticket", status,
                attempts, retries, "scheduler", null, null, version,
                Instant.parse("2026-09-28T00:00:00Z"), null, null,
                Instant.parse("2026-09-28T00:00:00Z"));
    }
}
