-- Сохраняет происхождение доставки после объединения партий.
CREATE TABLE dlh.bronze.batch_coalesce_event_map (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint
) WITH (format='PARQUET', format_version=2);
