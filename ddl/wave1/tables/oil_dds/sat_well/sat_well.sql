CREATE TABLE dlh.oil_dds.sat_well WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.sat_well;
