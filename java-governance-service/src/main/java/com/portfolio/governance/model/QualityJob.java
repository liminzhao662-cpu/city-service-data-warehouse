package com.portfolio.governance.model;

import java.time.Instant;

public record QualityJob(
        long jobId,
        String jobKey,
        String releaseKey,
        String assetKey,
        String status,
        int attemptCount,
        int maxRetries,
        String requestedBy,
        String errorMessage,
        Long dqRunId,
        int version,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        Instant updatedAt) {
}
