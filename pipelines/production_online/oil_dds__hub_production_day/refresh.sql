-- Создаём уникальные ключи скважино-дня по согласованному production business ID. Это целевой SQL-этап вместо части apply_batch GP.
WITH candidates AS
    (SELECT md5(concat('production:', e.entity_id)) AS production_hk,
            e.entity_id AS production_id,{{cut_time}} AS load_dts,
                                                    min(e.record_source) AS record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='production_day'
         AND NOT EXISTS
             (SELECT 1
              FROM {{initial:oil_dds__event}} i
              WHERE i.event_id=e.event_id)
     GROUP BY e.entity_id)
SELECT production_hk,
       production_id,
       load_dts,
       record_source
FROM {{initial:oil_dds__hub_production_day}}
UNION ALL
SELECT c.production_hk,
       c.production_id,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__hub_production_day}} i
         WHERE i.production_id=c.production_id);
