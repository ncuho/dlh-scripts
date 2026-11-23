-- Контекст миграции: AS IS / TO BE.
-- Назначение: Определяет справочник причин простоя для размерной модели.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.dim_downtime_reason WITH (
    format = 'PARQUET'
) AS
SELECT
    reason,
    description
FROM (
    VALUES (
            'EQUIPMENT', 'Оборудование'
),
        (
            'POWER', 'Энергоснабжение'
),
        (
            'PLANNED', 'Плановый простой'
)
) AS v (
        reason, description);
