-- Хранит устойчивый бизнес-ключ и hash key сущности production_day; атрибуты вынесены в satellites.
CREATE TABLE {{target:oil_dds__hub_production_day}} (
    "production_hk" varchar,
    "production_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
