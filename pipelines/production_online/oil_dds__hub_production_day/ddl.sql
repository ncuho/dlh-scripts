-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности production_day; атрибуты вынесены в satellites.
-- AS IS: oil_dds.hub_production_day в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_dds__hub_production_day в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__hub_production_day}} (
    "production_hk" varchar,
    "production_id" varchar,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar
) WITH (format='PARQUET', format_version=2);
