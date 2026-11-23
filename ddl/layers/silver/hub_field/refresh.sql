-- Хранит устойчивый бизнес-ключ и hash key сущности field; атрибуты вынесены в satellites.
CREATE TABLE dlh.silver.hub_field_next (
    "field_hk" varchar,
    "field_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
