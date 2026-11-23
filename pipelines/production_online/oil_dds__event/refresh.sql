-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит принятые event_id, версии сущностей, операции U/D и исходный документ.
-- AS IS: oil_dds.event в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: INITIAL не изменяется; source GP для пересчёта не перечитывается. Версии/удаления применяет целевой SQL.
-- Конец контекста миграции.
-- Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
WITH ranked AS
    (SELECT p.*,
            row_number() OVER (PARTITION BY event_id
                               ORDER BY topic, kafka_partition, kafka_offset, batch_id) AS rn
     FROM {{target:oil_stg__v_event_parsed}} p)
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
FROM {{initial:oil_dds__event}}
UNION ALL
SELECT p.event_id,
       p.event_type,
       p.entity_id,
       p.entity_version,
       p.op,
       p.event_time,{{cut_time}} AS load_dts,
                               p.topic AS record_source,
                               p.batch_id,
                               p.payload,
                               p.event_json AS event_document
FROM ranked p
WHERE p.rn=1
    AND NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__event}} i
         WHERE i.event_id=p.event_id);
