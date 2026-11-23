-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_mart.dim_well WITH (
    format = 'PARQUET'
) AS
SELECT
    w.well_hk,
    l.field_hk,
    w.entity_version,
    w.valid_from,
    lead(
        w.valid_from) OVER (PARTITION BY w.well_hk ORDER BY w.valid_from, w.entity_version) AS valid_to,
    w.well_name,
    w.status,
    (w.op = 'D') AS is_deleted
FROM
    dlh.oil_dds.sat_well w
    JOIN dlh.oil_dds.link_well_field l ON l.link_hk = w.link_hk;
