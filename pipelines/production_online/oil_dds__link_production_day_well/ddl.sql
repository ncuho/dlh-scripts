-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи production_day_well через hash keys связанных hubs.
-- AS IS: oil_dds.link_production_day_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_dds__link_production_day_well в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__link_production_day_well}} (
    "link_hk" varchar,
    "production_hk" varchar,
    "well_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
