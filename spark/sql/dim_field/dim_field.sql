-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('field:' || entity_id, 'utf-8'))))) AS field_hk,
  entity_version,
  event_time AS valid_from,
  LEAD(event_time) OVER (
    PARTITION BY entity_id
    ORDER BY event_time NULLS LAST, entity_version NULLS LAST, event_id NULLS LAST
  ) AS valid_to,
  GET_JSON_OBJECT(document, '$.payload.field_name') AS field_name,
  GET_JSON_OBJECT(document, '$.payload.region') AS region,
  (
    op = 'D'
  ) AS is_deleted
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'field'
