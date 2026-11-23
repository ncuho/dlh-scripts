-- Контекст миграции: AS IS / TO BE.
-- Назначение: Связывает исходные партии с объединённой партией аудита.
-- AS IS: oil_stg.batch_coalesce_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__batch_coalesce_map в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_coalesce_map}} (
    "original_batch_id" bigint,
    "coalesced_batch_id" bigint,
    "source_name" varchar,
    "original_event_count" bigint,
    "created_at" timestamp(6) with time zone
) WITH (format='PARQUET', format_version=2);
