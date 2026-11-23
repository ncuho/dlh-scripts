-- Хранит следующий принятый offset каждой исходной Kafka-партиции.
CREATE TABLE dlh.bronze.kafka_checkpoint_next (
    "topic" varchar,
    "kafka_partition" integer,
    "next_offset" bigint,
    "updated_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
