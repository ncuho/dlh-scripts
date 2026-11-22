SELECT
  p.production_date,
  p.field_hk,
  f.field_name,
  f.region,
  CAST(SUM(p.oil_tonnes) AS DECIMAL(22, 3)) AS oil_tonnes,
  CAST(SUM(CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END) AS BIGINT) AS producing_well_days,
  CASE
    WHEN SUM(CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END) = 0
    THEN CAST(NULL AS DECIMAL(22, 6))
    ELSE CAST(SUM(p.oil_tonnes) / SUM(CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END) AS DECIMAL(22, 6))
  END AS oil_per_well_day
FROM dlh.${schema}.fact_well_day AS p
JOIN dlh.${schema}.dim_field AS f
  ON f.field_hk = p.field_hk AND f.entity_version = p.field_version
GROUP BY
  p.production_date,
  p.field_hk,
  f.field_name,
  f.region
