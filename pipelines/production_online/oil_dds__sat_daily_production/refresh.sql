-- Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT production_hk,
       link_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       production_date,
       oil_tonnes,
       status
FROM {{initial:oil_dds__sat_daily_production}}
UNION ALL
SELECT md5(concat('production:', e.entity_id)) AS production_hk,
       md5(concat('production_day_well:', e.entity_id, ':', get_json_object(e.payload, '$.well_id'))) AS link_hk,
       e.event_id,
       e.entity_version,
       e.op,
       e.event_time,
       e.load_dts,
       e.record_source,
       md5(concat(e.op, ':', e.payload)) AS hashdiff,
       try_cast(get_json_object(e.payload, '$.production_date') AS DATE) AS production_date,
       try_cast(get_json_object(e.payload, '$.oil_tonnes') AS DECIMAL(18, 3)) AS oil_tonnes,
       get_json_object(e.payload, '$.status') AS status
FROM {{target:oil_dds__event}} e
WHERE e.event_type='production_day'
    AND NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__event}} i
         WHERE i.event_id=e.event_id);
