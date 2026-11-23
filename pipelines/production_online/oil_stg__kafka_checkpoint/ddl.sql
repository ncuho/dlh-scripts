-- Хранит следующий принятый offset каждой исходной Kafka-партиции.
CREATE TABLE {{target:oil_stg__kafka_checkpoint}} (
    "topic" varchar,
    "kafka_partition" integer,
    "next_offset" bigint,
    "updated_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
