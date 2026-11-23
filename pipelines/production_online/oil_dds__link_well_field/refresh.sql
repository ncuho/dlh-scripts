-- Из well payload получаем field ID и строим детерминированную связь ключей скважины/месторождения. Это целевой SQL-этап вместо части apply_batch GP.
WITH fresh AS
    (SELECT e.entity_id,
            get_json_object(e.payload, '$.field_id') AS field_id,
            e.record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='well'
         AND NOT EXISTS
             (SELECT 1
              FROM {{initial:oil_dds__event}} i
              WHERE i.event_id=e.event_id)),
     candidates AS
    (SELECT md5(concat('well_field:', entity_id, ':', field_id)) AS link_hk,
            md5(concat('well:', entity_id)) AS well_hk,
            md5(concat('field:', field_id)) AS field_hk,{{cut_time}} AS load_dts,
                                                                   min(record_source) AS record_source
     FROM fresh
     GROUP BY entity_id,
              field_id)
SELECT link_hk,
       well_hk,
       field_hk,
       load_dts,
       record_source
FROM {{initial:oil_dds__link_well_field}}
UNION ALL
SELECT c.link_hk,
       c.well_hk,
       c.field_hk,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__link_well_field}} i
         WHERE i.link_hk=c.link_hk);
