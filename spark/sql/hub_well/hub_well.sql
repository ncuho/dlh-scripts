-- Берём distinct бизнес-ключи скважин; изменяемые названия и статусы остаются в satellites. Это целевой SQL-этап вместо части apply_batch GP.
SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
