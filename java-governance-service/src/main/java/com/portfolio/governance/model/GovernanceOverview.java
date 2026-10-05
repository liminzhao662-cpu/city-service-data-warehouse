package com.portfolio.governance.model;

public record GovernanceOverview(
        long activeAssets,
        long lineageEdges,
        long enabledRules,
        QualityRun latestQualityRun
) {
}
