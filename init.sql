CREATE SCHEMA IF NOT EXISTS protein_etl;

CREATE TABLE IF NOT EXISTS protein_etl.start_info
(
    ID SERIAL PRIMARY KEY,
    de_version VARCHAR(255),
    git_commit_hash VARCHAR(255),
    start_date_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS protein_etl.data_quality
(
    ID SERIAL PRIMARY KEY,
    run_id INTEGER,
    source_table VARCHAR(255),
    protein_id VARCHAR(255),
    issue VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS protein_etl.run_summary
(
    ID SERIAL PRIMARY KEY,
    run_id INTEGER,
    end_date_time TIMESTAMP,
    n_proteins INTEGER,
    n_binding_records INTEGER,
    n_in_vivo_records INTEGER,
    n_candidate_proteins INTEGER,
    n_outlier_binding_records INTEGER,
    n_data_quality_issues INTEGER,
    status VARCHAR(50)
);
