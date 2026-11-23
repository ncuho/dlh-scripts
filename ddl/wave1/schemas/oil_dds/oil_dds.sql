-- Контекст миграции: AS IS / TO BE.
-- Назначение: Схема интеграции и истории бизнес-сущностей с hubs, links и satellites.
-- AS IS: Исходные схемы/таблицы GP с тем же бизнес-контрактом.
-- TO BE: Сохранённый вариант прямых Iceberg-копий в oil_stg/oil_dds/oil_mart.
-- Отличие: Текущий online-контракт использует префиксы внутри release namespace; не смешивайте эти DDL с production_online.
-- Конец контекста миграции.
CREATE SCHEMA IF NOT EXISTS dlh.oil_dds;
