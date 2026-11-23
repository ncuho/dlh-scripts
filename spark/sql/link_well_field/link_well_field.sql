-- Из well payload получаем field ID и строим детерминированную связь ключей скважины/месторождения. Это целевой SQL-этап вместо части apply_batch GP.
SELECT DISTINCT
  LOWER(
    HEX(
      UNHEX(
        MD5(
          ENCODE(
            'well_field:' || entity_id || ':' || GET_JSON_OBJECT(document, '$.payload.field_id'),
            'utf-8'
          )
        )
      )
    )
  ) AS link_hk,
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  LOWER(
    HEX(
      UNHEX(
        MD5(ENCODE('field:' || GET_JSON_OBJECT(document, '$.payload.field_id'), 'utf-8'))
      )
    )
  ) AS field_hk
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
