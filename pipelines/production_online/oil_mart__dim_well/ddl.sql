-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: oil_mart.dim_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_mart__dim_well в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__dim_well}} (
    "well_hk" varchar,
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" date,
    "valid_to" date,
    "well_name" varchar,
    "status" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);
