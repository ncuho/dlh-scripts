-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__event}} (
    "event_id" varchar,
    "event_type" varchar,
    "entity_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar,
    "batch_id" bigint,
    "payload" varchar,
    "event_document" varchar
) WITH (format='PARQUET', format_version=2);
