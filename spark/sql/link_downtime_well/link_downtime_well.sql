SELECT DISTINCT
  LOWER(
    HEX(
      UNHEX(
        MD5(
          ENCODE(
            'downtime_well:' || entity_id || ':' || GET_JSON_OBJECT(document, '$.payload.well_id'),
            'utf-8'
          )
        )
      )
    )
  ) AS link_hk,
  LOWER(HEX(UNHEX(MD5(ENCODE('downtime:' || entity_id, 'utf-8'))))) AS downtime_hk,
  LOWER(
    HEX(
      UNHEX(MD5(ENCODE('well:' || GET_JSON_OBJECT(document, '$.payload.well_id'), 'utf-8')))
    )
  ) AS well_hk
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'downtime'
