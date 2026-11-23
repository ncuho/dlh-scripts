-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит принятые event_id, версии сущностей, операции U/D и исходный документ.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_dds.event WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.event
WHERE
    event_type IN (
        'field', 'well', 'production_day');
