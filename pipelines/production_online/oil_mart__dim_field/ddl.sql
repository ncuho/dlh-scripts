-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: oil_mart.dim_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_mart__dim_field в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__dim_field}} (
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" timestamp(6) with time zone,
    "valid_to" timestamp(6) with time zone,
    "field_name" varchar,
    "region" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);
