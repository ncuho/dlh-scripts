CREATE TABLE dlh.oil_dds.hub_field WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_field;
