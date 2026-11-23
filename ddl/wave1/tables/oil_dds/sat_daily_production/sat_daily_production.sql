-- Хранит версии атрибутов daily_production, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.oil_dds.sat_daily_production WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.sat_daily_production;
