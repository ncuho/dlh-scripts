-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов well, происхождение, порядок версий и операции изменения.
-- AS IS: oil_dds.sat_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT well_hk,
       link_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       well_name,
       valid_from,
       status
FROM {{source:oil_dds__sat_well}};
