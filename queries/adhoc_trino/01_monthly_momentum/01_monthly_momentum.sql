/* Считает месячную добычу и её изменение к предыдущему месяцу по месторождениям. */
WITH monthly AS (
  SELECT
    CAST(DATE_TRUNC('MONTH', production_date) AS DATE) AS month,
    field_name,
    SUM(oil_tonnes) AS oil_tonnes
  FROM dlh.gold.v_field_production
  WHERE
    production_date >= CAST('2025-01-01' AS DATE)
    AND production_date < CAST('2026-01-01' AS DATE)
  GROUP BY
    1,
    2
), compared AS (
  SELECT
    *,
    LAG(oil_tonnes) OVER (PARTITION BY field_name ORDER BY month) AS previous_tonnes
  FROM monthly
)
SELECT
  month,
  field_name,
  oil_tonnes,
  ROUND(100.0 * (
    oil_tonnes - previous_tonnes
  ) / NULLIF(previous_tonnes, 0), 2) AS change_pct
FROM compared
ORDER BY
  field_name,
  month;
