-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__event_log}} (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "received_at" timestamp(6) with time zone,
    "event_json" varchar
) WITH (format='PARQUET', format_version=2);
