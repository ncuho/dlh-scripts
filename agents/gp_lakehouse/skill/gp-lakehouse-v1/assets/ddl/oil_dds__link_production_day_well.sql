-- Хранит историю связи production_day_well через hash keys связанных hubs.
CREATE TABLE dlh.silver.link_production_day_well (
    "link_hk" varchar,
    "production_hk" varchar,
    "well_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
