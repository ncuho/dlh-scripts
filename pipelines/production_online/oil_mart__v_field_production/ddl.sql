-- Контекст миграции: AS IS / TO BE.
-- Назначение: Предоставляет бизнес-колонки отчёта добычи по месторождениям.
-- AS IS: oil_mart.v_field_production в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_mart__v_field_production в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__v_field_production}} (
    "production_date" date,
    "field_hk" varchar,
    "field_name" varchar,
    "region" varchar,
    "oil_tonnes" decimal(22,3),
    "producing_well_days" bigint,
    "oil_per_well_day" decimal(22,6)
) WITH (format='PARQUET', format_version=2);
