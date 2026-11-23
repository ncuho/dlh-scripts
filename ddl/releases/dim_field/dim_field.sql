-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.dim_field WITH (
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
    entity_version,
    event_time AS valid_from,
    lead(
        event_time) OVER (PARTITION BY entity_id ORDER BY event_time, entity_version, event_id) AS valid_to,
    json_extract_scalar (document, '$.payload.field_name') AS field_name,
    json_extract_scalar (document, '$.payload.region') AS region,
    (op = 'D') AS is_deleted
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'field';
