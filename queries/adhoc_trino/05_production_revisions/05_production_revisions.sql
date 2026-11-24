/* Показывает число версий и изменение объёма добычи между самой ранней и последней версией события. */
WITH versions AS (
  SELECT
    production_hk,
    event_id,
    entity_version,
    op,
    production_date,
    oil_tonnes,
    ROW_NUMBER() OVER (PARTITION BY production_hk ORDER BY entity_version, event_id) AS first_n,
    ROW_NUMBER() OVER (
      PARTITION BY production_hk
      ORDER BY entity_version DESC NULLS FIRST, event_id DESC NULLS FIRST
    ) AS last_n,
    COUNT(*) OVER (PARTITION BY production_hk) AS revision_count
  FROM dlh.silver.sat_daily_production
)
SELECT
  production_hk,
  MIN(production_date) AS production_date,
  MAX(revision_count) AS revision_count,
  MAX(oil_tonnes) FILTER(WHERE
    first_n = 1) AS first_tonnes,
  MAX(oil_tonnes) FILTER(WHERE
    last_n = 1) AS latest_tonnes,
  MAX(oil_tonnes) FILTER(WHERE
    last_n = 1) - MAX(oil_tonnes) FILTER(WHERE
    first_n = 1) AS correction_tonnes,
  MAX(op) FILTER(WHERE
    last_n = 1) AS latest_op
FROM versions
GROUP BY
  production_hk
HAVING
  MAX(revision_count) > 1
ORDER BY
  revision_count DESC NULLS FIRST,
  production_hk
LIMIT 20;
