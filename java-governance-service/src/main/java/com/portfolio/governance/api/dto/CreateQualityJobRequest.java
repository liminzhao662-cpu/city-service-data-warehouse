package com.portfolio.governance.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateQualityJobRequest(
        @NotBlank @Size(max = 100) String jobKey,
        @NotBlank @Size(max = 100) String releaseKey,
        @NotBlank @Size(max = 80) String assetKey,
        @Min(0) @Max(5) Integer maxRetries,
        @Size(max = 80) String requestedBy) {
}
