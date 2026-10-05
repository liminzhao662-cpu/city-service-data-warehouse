USE nyc311;

CREATE TABLE IF NOT EXISTS pipeline_chunk (
    run_id BIGINT UNSIGNED NOT NULL,
    chunk_key VARCHAR(160) NOT NULL,
    input_sha256 CHAR(64) NOT NULL,
    row_count BIGINT UNSIGNED NOT NULL,
    inserted_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    unchanged_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    committed_at TIMESTAMP(3) NULL,
    PRIMARY KEY (run_id, chunk_key),
    CONSTRAINT fk_chunk_run FOREIGN KEY (run_id) REFERENCES pipeline_run(run_id),
    CONSTRAINT chk_chunk_status CHECK (status IN ('RUNNING','COMMITTED','FAILED'))
) ENGINE=InnoDB;
