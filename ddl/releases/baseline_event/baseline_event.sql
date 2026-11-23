-- Назначение: Неизменяемая копия полного реестра GP events на INITIAL cut. Один и тот же snapshot используется последующими REFRESH.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
-- INITIAL only: source is fully applied and held under the live GP/Kafka fence.
-- __INPUT_SCHEMA__ is a new immutable oil_i_* namespace selected by the DAG.
CREATE TABLE dlh.__INPUT_SCHEMA__.baseline_event
WITH (format = 'PARQUET') AS
SELECT *
FROM greenplum_gp.oil_dds.event;
