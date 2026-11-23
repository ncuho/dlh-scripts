-- Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
WITH fresh AS
    (SELECT e.entity_id,
            get_json_object(e.payload, '$.well_id') AS well_id,
            e.record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='production_day'
         AND NOT EXISTS
             (SELECT 1
              FROM {{initial:oil_dds__event}} i
              WHERE i.event_id=e.event_id)),
     candidates AS
    (SELECT md5(concat('production_day_well:', entity_id, ':', well_id)) AS link_hk,
            md5(concat('production:', entity_id)) AS production_hk,
            md5(concat('well:', well_id)) AS well_hk,{{cut_time}} AS load_dts,
                                                                min(record_source) AS record_source
     FROM fresh
     GROUP BY entity_id,
              well_id)
SELECT link_hk,
       production_hk,
       well_hk,
       load_dts,
       record_source
FROM {{initial:oil_dds__link_production_day_well}}
UNION ALL
SELECT c.link_hk,
       c.production_hk,
       c.well_hk,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__link_production_day_well}} i
         WHERE i.link_hk=c.link_hk);
