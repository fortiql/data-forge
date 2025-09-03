-- Gold table schema example
CREATE TABLE IF NOT EXISTS fact_edits_hourly (
    page_id BIGINT,
    user_id TEXT,
    hour TIMESTAMP,
    edit_count INT,
    size_delta BIGINT
);

CREATE TABLE IF NOT EXISTS dim_user_scd2 (
    user_id TEXT,
    is_bot BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
);
