-- Формирует SCD2-версии месторождений с интервалом действия.
CREATE TABLE {{target:oil_mart__dim_field}} (
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" timestamp(6) with time zone,
    "valid_to" timestamp(6) with time zone,
    "field_name" varchar,
    "region" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);
