-- Хранит принятые event_id, версии сущностей, операции U/D и исходный документ.
CREATE TABLE dlh.oil_dds.event WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.event
WHERE
    event_type IN (
        'field', 'well', 'production_day');
