SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
