-- Контекст миграции: AS IS / TO BE.
-- Назначение: Объединяет добычу и простои для отчёта эффективности.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.field_performance WITH (
    format = 'PARQUET'
) AS
SELECT
    p.*,
    coalesce(
        d.downtime_hours, DECIMAL '0.000000'
) AS downtime_hours
FROM
    dlh.oil_r_example.agg_field_day p
    LEFT JOIN dlh.oil_r_example.agg_field_downtime_day d ON d.production_date = p.production_date
        AND d.field_hk = p.field_hk;
