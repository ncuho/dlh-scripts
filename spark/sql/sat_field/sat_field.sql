-- Из field-событий строим field hash key и версии атрибутов; исходный payload определяет hashdiff. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('field:' || entity_id, 'utf-8'))))) AS field_hk,
  event_id,
  entity_version,
  op,
  event_time,
  document
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'field'
