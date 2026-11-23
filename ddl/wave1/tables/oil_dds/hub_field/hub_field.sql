-- Хранит устойчивый бизнес-ключ и hash key сущности field; атрибуты вынесены в satellites.
CREATE TABLE dlh.oil_dds.hub_field WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_field;
