-- Объединяет добычу и простои для отчёта эффективности.
CREATE TABLE dlh.oil_r_example.field_performance WITH (
    format = 'PARQUET'
) AS
SELECT
    p.*,
    coalesce(
        d.downtime_hours, DECIMAL '0.000000'
) AS downtime_hours
FROM
    dlh.oil_r_example.agg_field_day p
    LEFT JOIN dlh.oil_r_example.agg_field_downtime_day d ON d.production_date = p.production_date
        AND d.field_hk = p.field_hk;
