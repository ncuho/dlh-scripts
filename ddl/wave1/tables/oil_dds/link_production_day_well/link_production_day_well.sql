-- Хранит историю связи production_day_well через hash keys связанных hubs.
CREATE TABLE dlh.oil_dds.link_production_day_well WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.link_production_day_well;
