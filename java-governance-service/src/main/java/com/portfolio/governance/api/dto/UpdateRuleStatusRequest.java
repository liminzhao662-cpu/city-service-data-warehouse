package com.portfolio.governance.api.dto;

import jakarta.validation.constraints.NotNull;

public record UpdateRuleStatusRequest(@NotNull Boolean enabled) {
}
