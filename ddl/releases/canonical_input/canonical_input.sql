-- Объединяет закреплённую историю и ограниченное окно raw.
CREATE TABLE dlh.__RELEASE_SCHEMA__.canonical_input
WITH (format = 'PARQUET') AS
SELECT event_document AS document
FROM __BASELINE_TABLE__ FOR VERSION AS OF __BASELINE_SNAPSHOT__
UNION ALL
SELECT event_json AS document
FROM __DELTA_TABLE__ FOR VERSION AS OF __DELTA_SNAPSHOT__
WHERE topic = 'oil.events.v1'
  AND (__PARTITION_OFFSET_PREDICATES__);
