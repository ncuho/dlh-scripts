-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Берём distinct бизнес-ключи месторождений и вычисляем тот же md5 hash key, что и в GP. Это целевой SQL-этап вместо части apply_batch GP.
WITH candidates AS
    (SELECT md5(concat('field:', e.entity_id)) AS field_hk,
            e.entity_id AS field_id,{{release_time}} AS load_dts,
                                               min(e.record_source) AS record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='field'
         AND NOT EXISTS
             (SELECT 1
              FROM {{previous:oil_dds__event}} i
              WHERE i.event_id=e.event_id)
     GROUP BY e.entity_id)
SELECT field_hk,
       field_id,
       load_dts,
       record_source
FROM {{previous:oil_dds__hub_field}}
UNION ALL
SELECT c.field_hk,
       c.field_id,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{previous:oil_dds__hub_field}} i
         WHERE i.field_id=c.field_id);
