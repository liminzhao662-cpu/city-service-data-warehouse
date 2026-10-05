USE nyc311;

CREATE TABLE IF NOT EXISTS data_asset (
    asset_key VARCHAR(80) NOT NULL,
    asset_name_cn VARCHAR(160) NOT NULL,
    data_layer VARCHAR(30) NOT NULL,
    storage_type VARCHAR(30) NOT NULL,
    physical_name VARCHAR(255) NOT NULL,
    owner_name VARCHAR(80) NOT NULL,
    update_cycle VARCHAR(80) NOT NULL,
    primary_key_desc VARCHAR(255) NULL,
    partition_desc VARCHAR(255) NULL,
    description VARCHAR(500) NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    registered_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (asset_key),
    UNIQUE KEY uk_asset_physical_name (physical_name),
    CONSTRAINT chk_asset_status CHECK (status IN ('ACTIVE','INACTIVE'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS data_lineage (
    upstream_asset_key VARCHAR(80) NOT NULL,
    downstream_asset_key VARCHAR(80) NOT NULL,
    transform_name VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (upstream_asset_key, downstream_asset_key),
    CONSTRAINT fk_lineage_upstream FOREIGN KEY (upstream_asset_key) REFERENCES data_asset(asset_key),
    CONSTRAINT fk_lineage_downstream FOREIGN KEY (downstream_asset_key) REFERENCES data_asset(asset_key),
    CONSTRAINT chk_lineage_not_self CHECK (upstream_asset_key <> downstream_asset_key)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dq_rule (
    rule_key VARCHAR(20) NOT NULL,
    rule_name VARCHAR(160) NOT NULL,
    quality_dimension VARCHAR(40) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    comparison_operator VARCHAR(20) NOT NULL,
    threshold_value DECIMAL(20,6) NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (rule_key),
    CONSTRAINT chk_dq_severity CHECK (severity IN ('BLOCK','WARN'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dq_run (
    dq_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_key VARCHAR(100) NOT NULL,
    release_key VARCHAR(100) NOT NULL,
    asset_key VARCHAR(80) NOT NULL,
    status VARCHAR(20) NOT NULL,
    checked_rows BIGINT UNSIGNED NOT NULL,
    passed_rules INT UNSIGNED NOT NULL DEFAULT 0,
    warning_rules INT UNSIGNED NOT NULL DEFAULT 0,
    failed_rules INT UNSIGNED NOT NULL DEFAULT 0,
    started_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    finished_at TIMESTAMP(3) NULL,
    PRIMARY KEY (dq_run_id),
    UNIQUE KEY uk_dq_run_key (run_key),
    KEY idx_dq_release_status (release_key, status),
    CONSTRAINT fk_dq_asset FOREIGN KEY (asset_key) REFERENCES data_asset(asset_key),
    CONSTRAINT chk_dq_run_status CHECK (status IN ('RUNNING','PASSED','FAILED'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dq_result (
    dq_run_id BIGINT UNSIGNED NOT NULL,
    rule_key VARCHAR(20) NOT NULL,
    metric_value DECIMAL(20,6) NOT NULL,
    failure_rate DECIMAL(20,10) NOT NULL DEFAULT 0,
    result_status VARCHAR(20) NOT NULL,
    message VARCHAR(500) NULL,
    PRIMARY KEY (dq_run_id, rule_key),
    CONSTRAINT fk_dq_result_run FOREIGN KEY (dq_run_id) REFERENCES dq_run(dq_run_id),
    CONSTRAINT fk_dq_result_rule FOREIGN KEY (rule_key) REFERENCES dq_rule(rule_key),
    CONSTRAINT chk_dq_result_status CHECK (result_status IN ('PASS','WARN','FAIL'))
) ENGINE=InnoDB;
