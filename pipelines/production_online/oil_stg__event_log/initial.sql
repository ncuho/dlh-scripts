-- Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       kafka_offset,
       batch_id,
       received_at,
       event_json
FROM {{source:oil_stg__event_log}};
