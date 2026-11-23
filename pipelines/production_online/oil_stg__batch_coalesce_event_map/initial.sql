-- Сохраняет соответствие каждой Kafka-позиции исходной и объединённой партиям.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{source:oil_stg__batch_coalesce_event_map}};
