-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет неизменяемую исходную историю до границы INITIAL.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- INITIAL only: source is fully applied and held under the live GP/Kafka fence.
-- __INPUT_SCHEMA__ is a new immutable oil_i_* namespace selected by the DAG.
CREATE TABLE dlh.__INPUT_SCHEMA__.baseline_event
WITH (format = 'PARQUET') AS
SELECT *
FROM greenplum_gp.oil_dds.event;
