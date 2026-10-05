package com.portfolio.governance.model;

import java.time.Instant;

public record QualityRun(
        long runId,
        String runKey,
        String releaseKey,
        String assetKey,
        String status,
        long checkedRows,
        int passedRules,
        int warningRules,
        int failedRules,
        Instant startedAt,
        Instant finishedAt
) {
}
