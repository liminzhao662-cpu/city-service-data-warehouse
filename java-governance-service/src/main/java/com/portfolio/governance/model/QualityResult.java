package com.portfolio.governance.model;

import java.math.BigDecimal;

public record QualityResult(
        String ruleKey,
        String ruleName,
        String dimension,
        String severity,
        BigDecimal metricValue,
        BigDecimal failureRate,
        String status,
        String message
) {
}
