-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи well_field через hash keys связанных hubs.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_dds.link_well_field WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.link_well_field;
