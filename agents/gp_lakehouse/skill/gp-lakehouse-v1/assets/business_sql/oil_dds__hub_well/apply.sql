-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Берём distinct бизнес-ключи скважин; изменяемые названия и статусы остаются в satellites. Это целевой SQL-этап вместо части apply_batch GP.
WITH candidates AS
    (SELECT md5(concat('well:', e.entity_id)) AS well_hk,
            e.entity_id AS well_id,{{release_time}} AS load_dts,
                                              min(e.record_source) AS record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='well'
         AND NOT EXISTS
             (SELECT 1
              FROM {{previous:oil_dds__event}} i
              WHERE i.event_id=e.event_id)
     GROUP BY e.entity_id)
SELECT well_hk,
       well_id,
       load_dts,
       record_source
FROM {{previous:oil_dds__hub_well}}
UNION ALL
SELECT c.well_hk,
       c.well_id,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{previous:oil_dds__hub_well}} i
         WHERE i.well_id=c.well_id);
