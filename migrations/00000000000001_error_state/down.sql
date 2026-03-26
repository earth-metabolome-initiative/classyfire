PRAGMA foreign_keys = OFF;

DROP INDEX IF EXISTS idx_molecules_state_error;

CREATE TABLE molecules_old (
    inchikey TEXT PRIMARY KEY NOT NULL,
    inchi TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at BIGINT NULL,
    last_error TEXT NULL,
    classification_acquired_at BIGINT NULL,
    raw_json_zstd BLOB NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

INSERT INTO molecules_old (
    inchikey,
    inchi,
    state,
    attempts,
    next_retry_at,
    last_error,
    classification_acquired_at,
    raw_json_zstd,
    created_at,
    updated_at
)
SELECT
    inchikey,
    inchi,
    CASE
        WHEN state = 'error' THEN 'retry'
        ELSE state
    END,
    attempts,
    NULL,
    last_error,
    classification_acquired_at,
    raw_json_zstd,
    created_at,
    updated_at
FROM molecules;

DROP TABLE molecules;
ALTER TABLE molecules_old RENAME TO molecules;

DELETE FROM state_counts;
INSERT INTO state_counts (state, count)
SELECT state, COUNT(*)
FROM molecules
GROUP BY state;

CREATE INDEX idx_molecules_state_retry
    ON molecules(state, next_retry_at, updated_at);

PRAGMA foreign_keys = ON;
