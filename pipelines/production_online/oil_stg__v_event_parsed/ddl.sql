-- Контекст миграции: AS IS / TO BE.
-- Назначение: Извлекает поля envelope и payload без потери исходного JSON.
-- AS IS: oil_stg.v_event_parsed в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__v_event_parsed в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__v_event_parsed}} (
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
