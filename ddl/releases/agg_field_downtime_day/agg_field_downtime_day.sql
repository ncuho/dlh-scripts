-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.agg_field_downtime_day WITH (
    format = 'PARQUET'
) AS
SELECT
    CAST(
        at_timezone (
            start_at, 'UTC'
) AS DATE
) AS production_date,
    field_hk,
    CAST(
        sum(
            hours
) AS DECIMAL(
            20, 6
)
) AS downtime_hours
FROM
    dlh.oil_r_example.fact_downtime_interval
GROUP BY
    1,
    2;
