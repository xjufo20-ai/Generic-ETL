CREATE SCHEMA IF NOT EXISTS etl_output;

CREATE TABLE IF NOT EXISTS etl_output.agg_dept_salary (
    dept VARCHAR(255),
    total_salary DECIMAL(18,4),
    headcount BIGINT
);

CREATE TABLE IF NOT EXISTS etl_output.agg_sales_by_region_product (
    region VARCHAR(255),
    product VARCHAR(255),
    total_quantity BIGINT,
    total_revenue DECIMAL(18,4)
);

CREATE TABLE IF NOT EXISTS etl_output.pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL,
    row_count BIGINT DEFAULT 0,
    duration_ms BIGINT DEFAULT 0,
    error_message TEXT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP
);
