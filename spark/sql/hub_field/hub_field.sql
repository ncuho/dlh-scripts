SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('field:' || entity_id, 'utf-8'))))) AS field_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'field'
