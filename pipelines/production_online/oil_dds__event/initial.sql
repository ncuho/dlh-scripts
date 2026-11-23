-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит принятые event_id, версии сущностей, операции U/D и исходный документ.
-- AS IS: oil_dds.event в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT event_id,
       event_type,
       entity_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       batch_id,
       payload,
       event_document
FROM {{source:oil_dds__event}};
