/* Показывает долю месторождения в годовой добыче и продуктивность активного скважино-дня. */
SELECT
  field_name,
  region,
  SUM(oil_tonnes) AS oil_tonnes,
  SUM(producing_well_days) AS producing_well_days,
  ROUND(SUM(oil_tonnes) / NULLIF(SUM(producing_well_days), 0), 3) AS tonnes_per_well_day,
  ROUND(100.0 * SUM(oil_tonnes) / NULLIF(SUM(SUM(oil_tonnes)) OVER (), 0), 2) AS contribution_pct
FROM dlh.gold.v_field_production
WHERE
  production_date >= CAST('2025-01-01' AS DATE)
  AND production_date < CAST('2026-01-01' AS DATE)
GROUP BY
  field_name,
  region
ORDER BY
  oil_tonnes DESC NULLS FIRST;
