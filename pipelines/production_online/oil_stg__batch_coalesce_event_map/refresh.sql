-- Сохраняет соответствие каждой Kafka-позиции исходной и объединённой партиям.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{initial:oil_stg__batch_coalesce_event_map}};
