USE nyc311;

CREATE TABLE IF NOT EXISTS dq_job (
    job_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_key VARCHAR(100) NOT NULL,
    release_key VARCHAR(100) NOT NULL,
    asset_key VARCHAR(80) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    max_retries INT UNSIGNED NOT NULL DEFAULT 2,
    requested_by VARCHAR(80) NOT NULL,
    error_message VARCHAR(500) NULL,
    dq_run_id BIGINT UNSIGNED NULL,
    version_no INT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    started_at TIMESTAMP(3) NULL,
    finished_at TIMESTAMP(3) NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_dq_job_key (job_key),
    KEY idx_dq_job_status_created (status, created_at),
    KEY idx_dq_job_release_status (release_key, status),
    CONSTRAINT fk_dq_job_asset FOREIGN KEY (asset_key) REFERENCES data_asset(asset_key),
    CONSTRAINT fk_dq_job_run FOREIGN KEY (dq_run_id) REFERENCES dq_run(dq_run_id),
    CONSTRAINT chk_dq_job_status CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
    CONSTRAINT chk_dq_job_retry CHECK (max_retries <= 5)
) ENGINE=InnoDB;
