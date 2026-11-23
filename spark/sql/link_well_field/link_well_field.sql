-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
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
