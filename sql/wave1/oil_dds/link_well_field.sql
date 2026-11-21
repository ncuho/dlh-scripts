CREATE TABLE dlh.oil_dds.link_well_field WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.link_well_field;
