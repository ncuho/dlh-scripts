-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_coalesce_event_map}} (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint
) WITH (format='PARQUET', format_version=2);
