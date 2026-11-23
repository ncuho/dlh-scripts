-- Проецируем версионированные production-события: ключ, операция U/D, дата, Decimal-тонны и статус. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('production:' || entity_id, 'utf-8'))))) AS production_hk,
  event_id,
  entity_version,
  op,
  event_time,
  document
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'production_day'
