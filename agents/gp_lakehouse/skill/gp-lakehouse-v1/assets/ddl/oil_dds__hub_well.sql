-- Хранит устойчивый бизнес-ключ и hash key сущности well; атрибуты вынесены в satellites.
CREATE TABLE dlh.silver.hub_well (
    "well_hk" varchar,
    "well_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
