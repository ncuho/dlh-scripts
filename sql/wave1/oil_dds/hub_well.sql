CREATE TABLE dlh.oil_dds.hub_well WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_well;
