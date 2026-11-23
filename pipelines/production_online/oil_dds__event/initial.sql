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
