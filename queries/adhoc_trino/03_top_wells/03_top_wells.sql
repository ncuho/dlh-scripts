/* Выбирает десять скважин с максимальной добычей, связывая факт с точной исторической версией измерения. */
SELECT
  w.well_name,
  SUM(f.oil_tonnes) AS oil_tonnes,
  COUNT(*) FILTER(WHERE
    f.status = 'ACTIVE') AS producing_days,
  ROUND(AVG(f.oil_tonnes), 3) AS average_day_tonnes
FROM dlh.gold.fact_well_day AS f
JOIN dlh.gold.dim_well AS w
  ON w.well_hk = f.well_hk AND w.entity_version = f.well_version
WHERE
  f.production_date >= CAST('2025-01-01' AS DATE)
  AND f.production_date < CAST('2026-01-01' AS DATE)
GROUP BY
  w.well_name
ORDER BY
  oil_tonnes DESC NULLS FIRST
LIMIT 10;
