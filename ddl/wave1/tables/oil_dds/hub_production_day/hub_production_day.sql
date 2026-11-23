-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности production_day; атрибуты вынесены в satellites.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE TABLE dlh.oil_dds.hub_production_day WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM
    greenplum_gp.oil_dds.hub_production_day;
