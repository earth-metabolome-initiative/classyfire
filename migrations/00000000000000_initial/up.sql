CREATE TABLE molecules (
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

CREATE TABLE cid_map (
    cid BIGINT PRIMARY KEY NOT NULL,
    inchikey TEXT NOT NULL REFERENCES molecules(inchikey)
);

CREATE TABLE state_counts (
    state TEXT PRIMARY KEY NOT NULL,
    count BIGINT NOT NULL
);

CREATE TABLE taxonomy_counts (
    level TEXT NOT NULL,
    label TEXT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (level, label)
);

CREATE TABLE import_state (
    source_path TEXT PRIMARY KEY NOT NULL,
    source_size_bytes BIGINT NOT NULL,
    source_mtime_epoch BIGINT NOT NULL,
    last_committed_line BIGINT NOT NULL,
    last_committed_offset BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX idx_molecules_state_retry
    ON molecules(state, next_retry_at, updated_at);

CREATE INDEX idx_cid_map_inchikey
    ON cid_map(inchikey);
