SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('downtime:' || entity_id, 'utf-8'))))) AS downtime_hk,
  event_id,
  entity_version,
  op,
  event_time,
  document
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'downtime'
