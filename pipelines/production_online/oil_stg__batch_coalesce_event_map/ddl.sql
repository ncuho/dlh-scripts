-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет происхождение доставки после объединения партий.
-- AS IS: oil_stg.batch_coalesce_event_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__batch_coalesce_event_map в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_coalesce_event_map}} (
    "topic" varchar,
    "kafka_partition" integer,
    "kafka_offset" bigint,
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint
) WITH (format='PARQUET', format_version=2);
