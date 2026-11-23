-- Связывает исходные партии с объединённой партией аудита.
CREATE TABLE {{target:oil_stg__batch_coalesce_map}} (
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint,
    "source_name" varchar,
    "original_event_count" bigint,
    "created_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
