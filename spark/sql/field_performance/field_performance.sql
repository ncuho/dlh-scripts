-- Контекст миграции: AS IS / TO BE.
-- Назначение: Объединяет добычу и простои для отчёта эффективности.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT
  p.*,
  COALESCE(d.downtime_hours, CAST('0.000000' AS DECIMAL)) AS downtime_hours
FROM dlh.${schema}.agg_field_day AS p
LEFT JOIN dlh.${schema}.agg_field_downtime_day AS d
  ON d.production_date = p.production_date AND d.field_hk = p.field_hk
