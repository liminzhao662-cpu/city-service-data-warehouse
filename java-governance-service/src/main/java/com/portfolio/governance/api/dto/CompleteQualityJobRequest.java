package com.portfolio.governance.api.dto;

import jakarta.validation.constraints.Positive;

public record CompleteQualityJobRequest(@Positive Long dqRunId) {
}
