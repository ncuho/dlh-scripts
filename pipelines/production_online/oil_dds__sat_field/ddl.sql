-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов field, происхождение, порядок версий и операции изменения.
-- AS IS: oil_dds.sat_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus создаёт пустую физическую Iceberg-таблицу oil_dds__sat_field в namespace выпуска.
-- Отличие: View материализуется, если исходник — view; PK/FK/distribution не объявляются гарантиями Iceberg.
-- Конец контекста миграции.
-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_dds__sat_field}} (
    "field_hk" varchar,
    "event_id" varchar,
    "entity_version" bigint,
    "op" varchar,
    "event_time" timestamp(6) with time zone,
    "load_dts" timestamp(6) with time zone,
    "record_source" varchar,
    "hashdiff" varchar,
    "field_name" varchar,
    "region" varchar
) WITH (format='PARQUET', format_version=2);
