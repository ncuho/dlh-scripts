-- Хранит принятые event_id, версии сущностей, операции U/D и исходный документ.
CREATE TABLE dlh.silver.event (
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
