-- Контекст миграции: AS IS / TO BE.
-- Назначение: Объединяет закреплённую историю и ограниченное окно raw.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template; runner renders validated identifiers, snapshots and every partition window.
CREATE TABLE dlh.__RELEASE_SCHEMA__.canonical_input
WITH (format = 'PARQUET') AS
SELECT event_document AS document
FROM __BASELINE_TABLE__ FOR VERSION AS OF __BASELINE_SNAPSHOT__
UNION ALL
SELECT event_json AS document
FROM __DELTA_TABLE__ FOR VERSION AS OF __DELTA_SNAPSHOT__
WHERE topic = 'oil.events.v1'
  AND (__PARTITION_OFFSET_PREDICATES__);
