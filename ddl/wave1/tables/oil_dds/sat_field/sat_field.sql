-- Хранит версии атрибутов field, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.oil_dds.sat_field WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.sat_field;
