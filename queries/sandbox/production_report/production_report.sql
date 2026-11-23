-- Контекст миграции: AS IS / TO BE.
-- Назначение: Read-only запрос к опубликованному baseline первой цепочки через Cedrus. Сравнение с GP корректно только на одной границе.
-- AS IS: Исходные показатели вычисляются в GP.
-- TO BE: Read-only SQL показывает или сверяет принятые target-показатели через Cedrus.
-- Отличие: Имена таблиц и границы должны относиться к одному release; это не команда повторной загрузки.
-- Конец контекста миграции.
-- Cedrus: first chain, already materialized and read-only.
SELECT
    field_name,
    sum(oil_tonnes) AS oil_tonnes,
    sum(producing_well_days) AS active_well_days,
    sum(oil_tonnes) / nullif(sum(producing_well_days), 0) AS oil_per_active_well_day
FROM dlh.oil_mart.agg_field_day
WHERE production_date >= DATE '2025-01-01'
  AND production_date < DATE '2026-01-01'
GROUP BY field_name
ORDER BY oil_tonnes DESC;
