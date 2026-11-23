-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_control}} (
    "batch_id" bigint,
    "source_name" varchar,
    "created_at" timestamp(6) with time zone,
    "status" varchar,
    "source_manifest" varchar,
    "error_text" varchar
) WITH (format='PARQUET', format_version=2);
