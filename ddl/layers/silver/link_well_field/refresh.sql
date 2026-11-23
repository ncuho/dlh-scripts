-- Хранит историю связи well_field через hash keys связанных hubs.
CREATE TABLE dlh.silver.link_well_field_next (
    "link_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
