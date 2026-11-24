-- Создаёт целевой слой bronze.
CREATE SCHEMA IF NOT EXISTS dlh.bronze;

-- Создаёт целевой слой silver.
CREATE SCHEMA IF NOT EXISTS dlh.silver;

-- Создаёт целевой слой gold.
CREATE SCHEMA IF NOT EXISTS dlh.gold;


-- Учитывает состояния исходных партий и завершённость применения.
CREATE TABLE dlh.bronze.batch_control (
    "batch_id" bigint,
    "source_name" varchar,
    "created_at" timestamp(6) with time zone,
    "status" varchar,
    "source_manifest" varchar,
    "error_text" varchar
) WITH (format='PARQUET', format_version=2);


-- Связывает исходные партии с объединённой партией аудита.
CREATE TABLE dlh.bronze.batch_coalesce_map (
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint,
    "source_name" varchar,
    "original_event_count" bigint,
    "created_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);


-- Сохраняет JSON доставок с topic, partition, offset и исходной партией.
CREATE TABLE dlh.bronze.event_log (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "received_at" timestamp(6) with time zone,
    "event_json" varchar
) WITH (format='PARQUET', format_version=2);


-- Сохраняет происхождение доставки после объединения партий.
CREATE TABLE dlh.bronze.batch_coalesce_event_map (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint
) WITH (format='PARQUET', format_version=2);


-- Хранит следующий принятый offset каждой исходной Kafka-партиции.
CREATE TABLE dlh.bronze.kafka_checkpoint (
    "topic" varchar,
    "kafka_partition" integer,
    "next_offset" bigint,
    "updated_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);


-- Извлекает поля envelope и payload без потери исходного JSON.
CREATE TABLE dlh.bronze.v_event_parsed (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "event_json" varchar,
    "schema_version" integer,
    "event_id" varchar,
    "event_type" varchar,
    "entity_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "payload" varchar
) WITH (format='PARQUET', format_version=2);


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


-- Хранит устойчивый бизнес-ключ и hash key сущности field; атрибуты вынесены в satellites.
CREATE TABLE dlh.silver.hub_field (
    "field_hk" varchar,
    "field_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);


-- Хранит устойчивый бизнес-ключ и hash key сущности production_day; атрибуты вынесены в satellites.
CREATE TABLE dlh.silver.hub_production_day (
    "production_hk" varchar,
    "production_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);


-- Хранит устойчивый бизнес-ключ и hash key сущности well; атрибуты вынесены в satellites.
CREATE TABLE dlh.silver.hub_well (
    "well_hk" varchar,
    "well_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);


-- Хранит историю связи production_day_well через hash keys связанных hubs.
CREATE TABLE dlh.silver.link_production_day_well (
    "link_hk" varchar,
    "production_hk" varchar,
    "well_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);


-- Хранит историю связи well_field через hash keys связанных hubs.
CREATE TABLE dlh.silver.link_well_field (
    "link_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);


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


-- Хранит версии атрибутов well, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.silver.sat_well (
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


-- Формирует SCD2-версии месторождений с интервалом действия.
CREATE TABLE dlh.gold.dim_field (
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" timestamp(6) with time zone,
    "valid_to" timestamp(6) with time zone,
    "field_name" varchar,
    "region" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);


-- Формирует SCD2-версии скважин и принадлежность месторождению.
CREATE TABLE dlh.gold.dim_well (
    "well_hk" varchar,
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" date,
    "valid_to" date,
    "well_name" varchar,
    "status" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);


-- Связывает суточную добычу с действовавшими версиями измерений.
CREATE TABLE dlh.gold.fact_well_day (
    "production_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "production_date" date,
    "well_version" bigint,
    "field_version" bigint,
    "status" varchar,
    "oil_tonnes" decimal(18,3)
) WITH (format='PARQUET', format_version=2);


-- Суммирует тонны и активные скважино-дни по месторождению и дате.
CREATE TABLE dlh.gold.agg_field_day (
    "production_date" date,
    "field_hk" varchar,
    "field_name" varchar,
    "region" varchar,
    "oil_tonnes" decimal(22,3),
    "producing_well_days" bigint,
    "oil_per_well_day" decimal(22,6)
) WITH (format='PARQUET', format_version=2);


-- Предоставляет бизнес-колонки отчёта добычи по месторождениям.
CREATE TABLE dlh.gold.v_field_production (
    "production_date" date,
    "field_hk" varchar,
    "field_name" varchar,
    "region" varchar,
    "oil_tonnes" decimal(22,3),
    "producing_well_days" bigint,
    "oil_per_well_day" decimal(22,6)
) WITH (format='PARQUET', format_version=2);
