-- Контекст миграции: AS IS / TO BE.
-- Назначение: Выбирает согласованные версии событий и исключает точные повторы.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_r_example.canonical_event WITH (
    format = 'PARQUET'
) AS
SELECT
    document,
    json_extract_scalar (
        document, '$.event_id'
) AS event_id,
    json_extract_scalar (
        document, '$.event_type'
) AS event_type,
    json_extract_scalar (
        document, '$.entity_id'
) AS entity_id,
    CAST(
        json_extract_scalar (
            document, '$.entity_version'
) AS BIGINT
) AS entity_version,
    json_extract_scalar (
        document, '$.op'
) AS op,
    from_iso8601_timestamp (
        json_extract_scalar (
            document, '$.event_time'
)
) AS event_time
FROM (
    SELECT DISTINCT
        document
    FROM
        dlh.oil_r_example.canonical_input);
