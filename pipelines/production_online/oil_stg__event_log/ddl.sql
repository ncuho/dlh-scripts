-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет JSON доставок с topic, partition, offset и исходной партией.
-- AS IS: oil_stg.event_log в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__event_log в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__event_log}} (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "batch_id" bigint,
    "received_at" timestamp(6) with time zone,
    "event_json" varchar
) WITH (format='PARQUET', format_version=2);
