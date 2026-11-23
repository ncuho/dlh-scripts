-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_coalesce_map}} (
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint,
    "source_name" varchar,
    "original_event_count" bigint,
    "created_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
