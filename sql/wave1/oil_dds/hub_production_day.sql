CREATE TABLE dlh.oil_dds.hub_production_day WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_production_day;
