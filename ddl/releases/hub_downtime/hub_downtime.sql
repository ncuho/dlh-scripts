-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности downtime; атрибуты вынесены в satellites.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.hub_downtime WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'downtime:' || entity_id
)
)
)
) AS downtime_hk,
    entity_id
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'downtime';
