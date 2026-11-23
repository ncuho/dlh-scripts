-- Контекст миграции: AS IS / TO BE.
-- Назначение: Учитывает состояния исходных партий и завершённость применения.
-- AS IS: oil_stg.batch_control в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_stg__batch_control в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_stg__batch_control}} (
    "batch_id" bigint,
    "source_name" varchar,
    "created_at" timestamp(6) with time zone,
    "status" varchar,
    "source_manifest" varchar,
    "error_text" varchar
) WITH (format='PARQUET', format_version=2);
