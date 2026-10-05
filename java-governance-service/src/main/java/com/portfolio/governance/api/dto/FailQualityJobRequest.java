package com.portfolio.governance.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record FailQualityJobRequest(@NotBlank @Size(max = 500) String errorMessage) {
}
