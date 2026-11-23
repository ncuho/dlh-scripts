-- Контекст миграции: AS IS / TO BE.
-- Назначение: Принимает исходный JSON и Kafka-координаты отдельным Iceberg sink.
-- AS IS: GP sink записывает Stage и checkpoints в одной source-транзакции.
-- TO BE: Отдельный Iceberg sink сохраняет raw JSON с координатами Kafka.
-- Отличие: Raw commit не означает применение DDS/Marts; границы и дедупликация проверяются отдельно.
-- Конец контекста миграции.
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
