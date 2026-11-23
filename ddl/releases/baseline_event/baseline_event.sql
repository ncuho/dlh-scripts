-- Сохраняет неизменяемую исходную историю до границы INITIAL.
CREATE TABLE dlh.__INPUT_SCHEMA__.baseline_event
WITH (format = 'PARQUET') AS
SELECT *
FROM greenplum_gp.oil_dds.event;
