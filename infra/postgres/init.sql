-- Example Postgres initialization
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT,
    is_bot BOOLEAN
);
