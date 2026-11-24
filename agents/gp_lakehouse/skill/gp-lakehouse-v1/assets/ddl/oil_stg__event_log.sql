-- Сохраняет JSON доставок с topic, partition, offset и исходной партией.
CREATE TABLE dlh.bronze.event_log (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "received_at" timestamp(6) with time zone,
    "event_json" varchar
) WITH (format='PARQUET', format_version=2);
