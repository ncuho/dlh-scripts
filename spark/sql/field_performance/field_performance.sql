-- Соединяем дневные агрегаты добычи и простоев; показатели считаются на одном принятом выпуске. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  p.*,
  COALESCE(d.downtime_hours, CAST('0.000000' AS DECIMAL)) AS downtime_hours
FROM dlh.${schema}.agg_field_day AS p
LEFT JOIN dlh.${schema}.agg_field_downtime_day AS d
  ON d.production_date = p.production_date AND d.field_hk = p.field_hk
