-- Контекст миграции: AS IS / TO BE.
-- Назначение: Объединяет добычу и простои для отчёта эффективности.
-- AS IS: GP предоставляет view конечному отчёту.
-- TO BE: Cedrus предоставляет target serving над принятым Iceberg release.
-- Отличие: Binding меняется после сверок и grants; исходный отчёт не удаляется.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_demo_template.field_performance WITH (
    format = 'PARQUET'
) AS
SELECT
    production_date,
    field_hk,
    field_name,
    region,
    oil_tonnes,
    producing_well_days,
    oil_per_well_day,
    downtime_hours
FROM
    greenplum_gp.oil_mart.v_field_performance
WHERE
    false;
