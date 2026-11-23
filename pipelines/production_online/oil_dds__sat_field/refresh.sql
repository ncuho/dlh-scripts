-- Сохраняет версии атрибутов месторождения, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT field_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       field_name,
       region
FROM {{initial:oil_dds__sat_field}}
UNION ALL
SELECT md5(concat('field:', e.entity_id)) AS field_hk,
       e.event_id,
       e.entity_version,
       e.op,
       e.event_time,
       e.load_dts,
       e.record_source,
       md5(concat(e.op, ':', e.payload)) AS hashdiff,
       get_json_object(e.payload, '$.field_name') AS field_name,
       get_json_object(e.payload, '$.region') AS region
FROM {{target:oil_dds__event}} e
WHERE e.event_type='field'
    AND NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__event}} i
         WHERE i.event_id=e.event_id);
