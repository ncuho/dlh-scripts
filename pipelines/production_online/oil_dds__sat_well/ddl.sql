-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__sat_well}} (
    "well_hk" varchar,
    "link_hk" varchar,
    "event_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar,
    "hashdiff" varchar,
    "well_name" varchar,
    "valid_from" date,
    "status" varchar
) WITH (format='PARQUET', format_version=2);
