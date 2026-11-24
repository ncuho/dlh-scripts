-- Хранит версии атрибутов field, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.silver.sat_field (
    "field_hk" varchar,
    "event_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar,
    "hashdiff" varchar,
    "field_name" varchar,
    "region" varchar
) WITH (format='PARQUET', format_version=2);
