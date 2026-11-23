-- Сохраняет Kafka-доставки и исходный JSON в landing до бизнес-преобразований.
CREATE TABLE dlh.bronze.__raw_events (
    topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    batch_id bigint,
    received_at timestamp(6) with time zone,
    event_json varchar
) WITH (format='PARQUET', format_version=2);
