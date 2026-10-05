package com.portfolio.governance.model;

public record LineageEdge(
        String direction,
        String relatedAssetKey,
        String relatedAssetName,
        String relatedDataLayer,
        String transformName
) {
}
