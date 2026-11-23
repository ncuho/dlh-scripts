-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
SELECT
  CAST(FROM_UTC_TIMESTAMP(start_at, 'UTC') AS DATE) AS production_date,
  field_hk,
  CAST(SUM(hours) AS DECIMAL(20, 6)) AS downtime_hours
FROM dlh.${schema}.fact_downtime_interval
GROUP BY
  1,
  2
