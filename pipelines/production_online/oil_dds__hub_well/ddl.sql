-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности well; атрибуты вынесены в satellites.
-- AS IS: oil_dds.hub_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_dds__hub_well в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__hub_well}} (
    "well_hk" varchar,
    "well_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
