-- Выбираем последнюю неудалённую версию простоя, связываем с активным фактом добычи и считаем часы. Это целевой SQL-этап вместо части apply_batch GP.
WITH d AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY entity_version DESC, event_id DESC) AS rn
  FROM dlh.${schema}.canonical_event
  WHERE
    event_type = 'downtime'
), decoded AS (
  SELECT
    LOWER(HEX(UNHEX(MD5(ENCODE('downtime:' || entity_id, 'utf-8'))))) AS downtime_hk,
    LOWER(
      HEX(
        UNHEX(MD5(ENCODE('well:' || GET_JSON_OBJECT(document, '$.payload.well_id'), 'utf-8')))
      )
    ) AS well_hk,
    TO_TIMESTAMP(GET_JSON_OBJECT(document, '$.payload.start_at')) AS start_at,
    TO_TIMESTAMP(GET_JSON_OBJECT(document, '$.payload.end_at')) AS end_at,
    GET_JSON_OBJECT(document, '$.payload.reason') AS reason
  FROM d
  WHERE
    rn = 1 AND op = 'U'
)
SELECT
  d.downtime_hk,
  d.well_hk,
  p.field_hk,
  d.start_at,
  d.end_at,
  d.reason,
  CAST((
    (UNIX_MICROS(d.end_at) / 1000000.0) - (UNIX_MICROS(d.start_at) / 1000000.0)
  ) / 3600 AS DECIMAL(16, 6)) AS hours
FROM decoded AS d
JOIN dlh.${schema}.fact_well_day AS p
  ON p.well_hk = d.well_hk
  AND p.production_date = CAST(FROM_UTC_TIMESTAMP(d.start_at, 'UTC') AS DATE)
  AND p.status = 'ACTIVE'
