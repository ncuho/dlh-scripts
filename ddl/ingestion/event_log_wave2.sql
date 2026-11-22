CREATE TABLE dlh.oil_stg.event_log_wave2 (
    topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    batch_id bigint,
    received_at timestamp(6) with time zone,
    event_json varchar
)
WITH (
    format = 'PARQUET'
);
