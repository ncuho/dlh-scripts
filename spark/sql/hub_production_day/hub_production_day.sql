-- Создаём уникальные ключи скважино-дня по согласованному production business ID. Это целевой SQL-этап вместо части apply_batch GP.
SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('production:' || entity_id, 'utf-8'))))) AS production_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'production_day'
