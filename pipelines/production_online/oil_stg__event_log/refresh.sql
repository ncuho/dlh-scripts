-- Собираем INITIAL и raw-окно, выбирая одну доставку по topic/partition/offset; исходный baseline остаётся неизменным.
WITH r AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            batch_id,
            received_at,
            event_json,
            0 AS origin
     FROM {{initial:oil_stg__event_log}}
     UNION ALL SELECT topic,
                      kafka_partition,
                      kafka_offset,
                      batch_id,
                      received_at,
                      event_json,
                      1 AS origin
     FROM {{delta}}),
     ranked AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            batch_id,
            received_at,
            event_json,
            row_number() OVER (PARTITION BY topic, kafka_partition, kafka_offset
                               ORDER BY origin, received_at, batch_id, event_json) AS rn
     FROM r)
SELECT topic,
       kafka_partition,
       kafka_offset,
       batch_id,
       received_at,
       event_json
FROM ranked
WHERE rn=1;
