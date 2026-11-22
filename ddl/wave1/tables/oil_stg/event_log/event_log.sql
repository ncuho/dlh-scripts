CREATE TABLE dlh.oil_stg.event_log WITH (
    format = 'PARQUET'
) AS
SELECT
    e.*
FROM
    greenplum_gp.oil_stg.event_log e
    JOIN greenplum_gp.oil_stg.v_event_parsed p ON e.topic = p.topic
        AND e.kafka_partition = p.kafka_partition
        AND e.kafka_offset = p.kafka_offset
WHERE
    p.event_type IN (
        'field', 'well', 'production_day');
