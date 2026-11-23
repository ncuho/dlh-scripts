-- Создаём ключи интервалов простоя дополнительной ветки; в первую целевую волну не включается. Это целевой SQL-этап вместо части apply_batch GP.
SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('downtime:' || entity_id, 'utf-8'))))) AS downtime_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'downtime'
