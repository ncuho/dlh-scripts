-- Из well-событий строим well/link hash keys, сохраняем версии и U/D; hashdiff зависит от исходного текста payload. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  event_id,
  entity_version,
  op,
  event_time,
  document
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
