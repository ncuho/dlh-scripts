-- Контекст миграции: AS IS / TO BE.
-- Назначение: Связывает исходные партии с объединённой партией аудита.
-- AS IS: oil_stg.batch_coalesce_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: Сохраняется аудит GP на C0; это не новый checkpoint Iceberg consumer и не live source C1.
-- Конец контекста миграции.
-- Связывает исходные партии с объединённой партией и сохраняет число исходных доставок.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT original_batch_id,
       coalesced_batch_id,
       source_name,
       original_event_count,
       created_at
FROM {{initial:oil_stg__batch_coalesce_map}};
