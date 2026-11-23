-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит следующий принятый offset каждой исходной Kafka-партиции.
-- AS IS: oil_stg.kafka_checkpoint в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__kafka_checkpoint в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__kafka_checkpoint}} (
    "topic" varchar,
    "kafka_partition" integer,
    "next_offset" bigint,
    "updated_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
