-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи well_field через hash keys связанных hubs.
-- AS IS: oil_dds.link_well_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_dds__link_well_field в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__link_well_field}} (
    "link_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
