-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
