-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
SELECT
  p.*,
  COALESCE(d.downtime_hours, CAST('0.000000' AS DECIMAL)) AS downtime_hours
FROM dlh.${schema}.agg_field_day AS p
LEFT JOIN dlh.${schema}.agg_field_downtime_day AS d
  ON d.production_date = p.production_date AND d.field_hk = p.field_hk
