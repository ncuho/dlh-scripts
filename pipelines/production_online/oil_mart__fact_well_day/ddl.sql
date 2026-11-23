-- Контекст миграции: AS IS / TO BE.
-- Назначение: Связывает суточную добычу с действовавшими версиями измерений.
-- AS IS: oil_mart.fact_well_day в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_mart__fact_well_day в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__fact_well_day}} (
    "production_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "production_date" date,
    "well_version" bigint,
    "field_version" bigint,
    "status" varchar,
    "oil_tonnes" decimal(18,3)
) WITH (format='PARQUET', format_version=2);
