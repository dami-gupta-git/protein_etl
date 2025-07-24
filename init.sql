CREATE SCHEMA IF NOT EXISTS protein_etl;

CREATE TABLE IF NOT EXISTS protein_etl.start_info
(
    ID SERIAL PRIMARY KEY,
    de_version VARCHAR(255),
    git_commit_hash VARCHAR(255),
    start_date_time TIMESTAMP
);
