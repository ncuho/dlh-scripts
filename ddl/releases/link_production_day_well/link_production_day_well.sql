-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи production_day_well через hash keys связанных hubs.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.link_production_day_well WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'production_day_well:' || entity_id || ':' || json_extract_scalar (
                        document, '$.payload.well_id'
)
)
)
)
) AS link_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'production:' || entity_id
)
)
)
) AS production_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'well:' || json_extract_scalar (
                        document, '$.payload.well_id'
)
)
)
)
) AS well_hk
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'production_day';
