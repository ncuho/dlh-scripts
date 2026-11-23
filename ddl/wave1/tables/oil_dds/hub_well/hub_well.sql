-- Хранит устойчивый бизнес-ключ и hash key сущности well; атрибуты вынесены в satellites.
CREATE TABLE dlh.oil_dds.hub_well WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_well;
