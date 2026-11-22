SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  LOWER(
    HEX(
      UNHEX(
        MD5(ENCODE('field:' || GET_JSON_OBJECT(document, '$.payload.field_id'), 'utf-8'))
      )
    )
  ) AS field_hk,
  entity_version,
  CAST(GET_JSON_OBJECT(document, '$.payload.valid_from') AS DATE) AS valid_from,
  LEAD(CAST(GET_JSON_OBJECT(document, '$.payload.valid_from') AS DATE)) OVER (
    PARTITION BY entity_id
    ORDER BY CAST(GET_JSON_OBJECT(document, '$.payload.valid_from') AS DATE) NULLS LAST, entity_version NULLS LAST, event_id NULLS LAST
  ) AS valid_to,
  GET_JSON_OBJECT(document, '$.payload.well_name') AS well_name,
  GET_JSON_OBJECT(document, '$.payload.status') AS status,
  (
    op = 'D'
  ) AS is_deleted
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
