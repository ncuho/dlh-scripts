-- Разбирает envelope JSON в STG: тип, ID, версию, операцию, время и payload без изменения исходного документа.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT batch_id,
       topic,
       kafka_partition,
       kafka_offset,
       event_json,
       try_cast(get_json_object(event_json, '$.schema_version') AS INT) AS schema_version,
       get_json_object(event_json, '$.event_id') AS event_id,
       get_json_object(event_json, '$.event_type') AS event_type,
       get_json_object(event_json, '$.entity_id') AS entity_id,
       try_cast(get_json_object(event_json, '$.entity_version') AS BIGINT) AS entity_version,
       get_json_object(event_json, '$.op') AS op,
       try_cast(get_json_object(event_json, '$.event_time') AS TIMESTAMP) AS event_time,
       bridge_json_payload(event_json) AS payload
FROM {{target:oil_stg__event_log}};
