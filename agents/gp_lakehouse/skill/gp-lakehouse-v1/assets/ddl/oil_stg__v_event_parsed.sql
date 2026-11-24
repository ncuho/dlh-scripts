-- Извлекает поля envelope и payload без потери исходного JSON.
CREATE TABLE dlh.bronze.v_event_parsed (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "event_json" varchar,
    "schema_version" integer,
    "event_id" varchar,
    "event_type" varchar,
    "entity_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "payload" varchar
) WITH (format='PARQUET', format_version=2);
