-- Собираем INITIAL и raw-окно, выбирая одну доставку по topic/partition/offset; исходный baseline остаётся неизменным.
SELECT topic,
       kafka_partition,
       kafka_offset,
       batch_id,
       received_at,
       event_json
FROM {{source:oil_stg__event_log}};
