package com.portfolio.governance.model;

import java.math.BigDecimal;
import java.time.Instant;

public record QualityRule(
        String ruleKey,
        String ruleName,
        String qualityDimension,
        String severity,
        String metricName,
        String comparisonOperator,
        BigDecimal thresholdValue,
        boolean enabled,
        Instant updatedAt) {
}
