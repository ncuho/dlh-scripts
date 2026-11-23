-- Контекст миграции: AS IS / TO BE.
-- Назначение: Суммирует тонны и активные скважино-дни по месторождению и дате.
-- AS IS: oil_mart.agg_field_day в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_mart__agg_field_day в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__agg_field_day}} (
    "production_date" date,
    "field_hk" varchar,
    "field_name" varchar,
    "region" varchar,
    "oil_tonnes" decimal(22,3),
    "producing_well_days" bigint,
    "oil_per_well_day" decimal(22,6)
) WITH (format='PARQUET', format_version=2);
