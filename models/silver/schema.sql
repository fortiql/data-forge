-- Silver table schema example
CREATE TABLE IF NOT EXISTS silver_recentchanges (
    page_id BIGINT,
    user_id TEXT,
    edits INT,
    ts TIMESTAMP
);
