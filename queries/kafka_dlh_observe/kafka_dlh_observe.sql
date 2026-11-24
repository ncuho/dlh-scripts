-- Показывает принятые Iceberg sink события: повторите запрос через 15–30 секунд при работающем producer.
SELECT topic,kafka_partition,count(*) AS raw_rows,count(DISTINCT kafka_offset) AS unique_positions,
       max(kafka_offset)+1 AS received_end,max(received_at) AS last_received_at
FROM dlh.bronze.__raw_events
GROUP BY topic,kafka_partition ORDER BY topic,kafka_partition;


-- Показывает хвост между доставкой в raw и применением бизнес-DAG; end — первый offset после диапазона.
WITH received AS (
    SELECT topic,kafka_partition,max(kafka_offset)+1 AS received_end
    FROM dlh.bronze.__raw_events GROUP BY topic,kafka_partition
)
SELECT r.topic,r.kafka_partition,r.received_end,c.next_offset AS applied_end,
       greatest(r.received_end-coalesce(c.next_offset,0),0) AS pending_positions
FROM received r LEFT JOIN dlh.bronze.kafka_checkpoint c
    ON c.topic=r.topic AND c.kafka_partition=r.kafka_partition
ORDER BY r.topic,r.kafka_partition;


-- Показывает последние документы raw; высокая entity_version здесь ещё не означает обновление Gold.
SELECT received_at,kafka_partition,kafka_offset,
       json_extract_scalar(event_json,'$.event_id') AS event_id,
       json_extract_scalar(event_json,'$.event_type') AS event_type,
       json_extract_scalar(event_json,'$.entity_id') AS entity_id,
       json_extract_scalar(event_json,'$.entity_version') AS entity_version,
       json_extract_scalar(event_json,'$.payload.oil_tonnes') AS oil_tonnes
FROM dlh.bronze.__raw_events ORDER BY received_at DESC,kafka_partition,kafka_offset DESC LIMIT 20;


-- Показывает применённые бизнес-версии в Silver; после catchup/release они продвигаются вслед за raw.
SELECT event_type,max(entity_version) AS latest_applied_version,count(*) AS event_versions
FROM dlh.silver.event GROUP BY event_type ORDER BY event_type;


-- Показывает результат витрины, который читает целевой Superset; обновляется после успешной обработки Spark.
SELECT count(*) AS field_days,sum(oil_tonnes) AS oil_tonnes,sum(producing_well_days) AS producing_well_days
FROM dlh.gold.v_field_production;
