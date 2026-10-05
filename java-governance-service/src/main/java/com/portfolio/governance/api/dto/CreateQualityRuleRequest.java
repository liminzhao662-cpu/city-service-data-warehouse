package com.portfolio.governance.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateQualityRuleRequest(
        @NotBlank @Size(max = 20) @Pattern(regexp = "[A-Za-z0-9_-]+") String ruleKey,
        @NotBlank @Size(max = 160) String ruleName,
        @NotBlank @Size(max = 40) String qualityDimension,
        @NotBlank @Pattern(regexp = "BLOCK|WARN") String severity,
        @NotBlank @Size(max = 100) String metricName,
        @NotBlank @Pattern(regexp = "eq|lte|rate_lte") String comparisonOperator,
        @NotNull @DecimalMin("0") BigDecimal thresholdValue) {
}
