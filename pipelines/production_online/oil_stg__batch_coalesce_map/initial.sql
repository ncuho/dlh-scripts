-- Контекст миграции: AS IS / TO BE.
-- Назначение: Связывает исходные партии с объединённой партией аудита.
-- AS IS: oil_stg.batch_coalesce_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Связывает исходные партии с объединённой партией и сохраняет число исходных доставок.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT original_batch_id,
       coalesced_batch_id,
       source_name,
       original_event_count,
       created_at
FROM {{source:oil_stg__batch_coalesce_map}};
