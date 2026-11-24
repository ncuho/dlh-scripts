-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Ранжируем доставки event_id; точный replay не создаёт второе событие, исходный INITIAL-аудит сохраняется. Это целевой SQL-этап вместо части apply_batch GP.
WITH ranked AS
    (SELECT p.*,
            row_number() OVER (PARTITION BY event_id
                               ORDER BY topic, kafka_partition, kafka_offset, batch_id) AS rn
     FROM {{target:oil_stg__v_event_parsed}} p)
SELECT event_id,
       event_type,
       entity_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       batch_id,
       payload,
       event_document
FROM {{previous:oil_dds__event}}
UNION ALL
SELECT p.event_id,
       p.event_type,
       p.entity_id,
       p.entity_version,
       p.op,
       p.event_time,{{release_time}} AS load_dts,
                               p.topic AS record_source,
                               p.batch_id,
                               p.payload,
                               p.event_json AS event_document
FROM ranked p
WHERE p.rn=1
    AND NOT EXISTS
        (SELECT 1
         FROM {{previous:oil_dds__event}} i
         WHERE i.event_id=p.event_id);
