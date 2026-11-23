-- Суммируем часы интервалов по месторождению и календарной дате UTC. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  CAST(FROM_UTC_TIMESTAMP(start_at, 'UTC') AS DATE) AS production_date,
  field_hk,
  CAST(SUM(hours) AS DECIMAL(20, 6)) AS downtime_hours
FROM dlh.${schema}.fact_downtime_interval
GROUP BY
  1,
  2
