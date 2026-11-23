-- Контекст миграции: AS IS / TO BE.
-- Назначение: Агрегирует часы и причины простоев по месторождению и дате.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT
  CAST(FROM_UTC_TIMESTAMP(start_at, 'UTC') AS DATE) AS production_date,
  field_hk,
  CAST(SUM(hours) AS DECIMAL(20, 6)) AS downtime_hours
FROM dlh.${schema}.fact_downtime_interval
GROUP BY
  1,
  2
