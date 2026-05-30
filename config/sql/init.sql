CREATE SCHEMA IF NOT EXISTS etl_output;

-- Aggregated department salaries
CREATE TABLE IF NOT EXISTS etl_output.agg_dept_salary (
    dept VARCHAR(255),
    total_salary DECIMAL(18,4),
    headcount BIGINT
);

-- Aggregated sales by region/product
CREATE TABLE IF NOT EXISTS etl_output.agg_sales_by_region_product (
    region VARCHAR(255),
    product VARCHAR(255),
    total_quantity BIGINT,
    total_revenue DECIMAL(18,4)
);

-- Pipeline execution history
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

-- Daily trade statistics (local cache — avoids re-fetching from source)
CREATE TABLE IF NOT EXISTS etl_output.trade_daily_stats (
    trade_date DATE NOT NULL,
    daily_volume DECIMAL(18,4) DEFAULT 0,
    trade_count BIGINT DEFAULT 0,
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date)
);

-- Weekly trade view (materialized from daily stats)
CREATE OR REPLACE VIEW etl_output.trade_weekly_stats AS
SELECT
    DATE_TRUNC('week', trade_date) AS week_start,
    SUM(daily_volume) AS weekly_volume,
    SUM(trade_count) AS weekly_trade_count,
    COUNT(*) AS days_with_data
FROM etl_output.trade_daily_stats
GROUP BY DATE_TRUNC('week', trade_date)
ORDER BY week_start;

-- Monthly trade view
CREATE OR REPLACE VIEW etl_output.trade_monthly_stats AS
SELECT
    DATE_TRUNC('month', trade_date) AS month_start,
    SUM(daily_volume) AS monthly_volume,
    SUM(trade_count) AS monthly_trade_count,
    COUNT(*) AS days_with_data
FROM etl_output.trade_daily_stats
GROUP BY DATE_TRUNC('month', trade_date)
ORDER BY month_start;
