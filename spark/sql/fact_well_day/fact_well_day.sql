-- Row_number выбирает последнюю production-версию; U/D и исторические интервалы dim_well/dim_field определяют единственную строку факта. Расчёт выполняется вместо соответствующей части refresh_production GP.
WITH p AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY entity_version DESC, event_id DESC) AS rn
  FROM dlh.${schema}.canonical_event
  WHERE
    event_type = 'production_day'
), decoded AS (
  SELECT
    LOWER(HEX(UNHEX(MD5(ENCODE('production:' || entity_id, 'utf-8'))))) AS production_hk,
    LOWER(
      HEX(
        UNHEX(MD5(ENCODE('well:' || GET_JSON_OBJECT(document, '$.payload.well_id'), 'utf-8')))
      )
    ) AS well_hk,
    CAST(GET_JSON_OBJECT(document, '$.payload.production_date') AS DATE) AS production_date,
    CAST(GET_JSON_OBJECT(document, '$.payload.oil_tonnes') AS DECIMAL(18, 3)) AS oil_tonnes,
    GET_JSON_OBJECT(document, '$.payload.status') AS status
  FROM p
  WHERE
    rn = 1 AND op = 'U'
)
SELECT
  p.production_hk,
  p.well_hk,
  w.field_hk,
  p.production_date,
  w.entity_version AS well_version,
  f.entity_version AS field_version,
  p.status,
  p.oil_tonnes
FROM decoded AS p
JOIN dlh.${schema}.dim_well AS w
  ON w.well_hk = p.well_hk
  AND NOT w.is_deleted
  AND w.valid_from <= p.production_date
  AND (
    w.valid_to IS NULL OR p.production_date < w.valid_to
  )
JOIN dlh.${schema}.dim_field AS f
  ON f.field_hk = w.field_hk
  AND NOT f.is_deleted
  AND f.valid_from <= (
    FROM_UTC_TIMESTAMP(CAST(p.production_date AS TIMESTAMP), 'UTC')
  )
  AND (
    f.valid_to IS NULL
    OR (
      FROM_UTC_TIMESTAMP(CAST(p.production_date AS TIMESTAMP), 'UTC')
    ) < f.valid_to
  )
