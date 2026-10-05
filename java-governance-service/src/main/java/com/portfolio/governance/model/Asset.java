package com.portfolio.governance.model;

import java.time.Instant;

public record Asset(
        String assetKey,
        String assetName,
        String dataLayer,
        String storageType,
        String physicalName,
        String owner,
        String updateCycle,
        String primaryKey,
        String partitioning,
        String description,
        String status,
        Instant updatedAt
) {
}
