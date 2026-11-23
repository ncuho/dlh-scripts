-- Учитывает состояния исходных партий и завершённость применения.
CREATE TABLE dlh.bronze.batch_control_next (
    "batch_id" bigint,
    "source_name" varchar,
    "created_at" timestamp(6) with time zone,
    "status" varchar,
    "source_manifest" varchar,
    "error_text" varchar
) WITH (format='PARQUET', format_version=2);
