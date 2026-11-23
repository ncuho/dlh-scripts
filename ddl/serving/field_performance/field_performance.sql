-- Назначение: Физическая serving-таблица второй цепочки. Объединяет добычу и простои по дате/месторождению и используется целевым BI.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
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
