-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов field, происхождение, порядок версий и операции изменения.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.sat_field WITH (
    format = 'PARQUET'
) AS
SELECT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'field:' || entity_id
)
)
)
) AS field_hk,
    event_id,
    entity_version,
    op,
    event_time,
    document
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'field';
