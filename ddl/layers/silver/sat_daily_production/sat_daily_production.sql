-- Хранит версии атрибутов daily_production, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.silver.sat_daily_production (
    "production_hk" varchar,
    "link_hk" varchar,
    "event_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar,
    "hashdiff" varchar,
    "production_date" date,
    "oil_tonnes" decimal(18,3),
    "status" varchar
) WITH (format='PARQUET', format_version=2);
