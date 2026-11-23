-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_mart.dim_field WITH (
    format = 'PARQUET'
) AS
SELECT
    field_hk,
    entity_version,
    event_time AS valid_from,
    lead(
        event_time) OVER (PARTITION BY field_hk ORDER BY event_time, entity_version) AS valid_to,
    field_name,
    region,
    (op = 'D') AS is_deleted
FROM
    dlh.oil_dds.sat_field;
