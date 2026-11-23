-- Из well-событий строим well/link hash keys, сохраняем версии и U/D; hashdiff зависит от исходного текста payload. Это целевой SQL-этап вместо части apply_batch GP.
SELECT well_hk,
       link_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       well_name,
       valid_from,
       status
FROM {{initial:oil_dds__sat_well}}
UNION ALL
SELECT md5(concat('well:', e.entity_id)) AS well_hk,
       md5(concat('well_field:', e.entity_id, ':', get_json_object(e.payload, '$.field_id'))) AS link_hk,
       e.event_id,
       e.entity_version,
       e.op,
       e.event_time,
       e.load_dts,
       e.record_source,
       md5(concat(e.op, ':', e.payload)) AS hashdiff,
       get_json_object(e.payload, '$.well_name') AS well_name,
       try_cast(get_json_object(e.payload, '$.valid_from') AS DATE) AS valid_from,
       get_json_object(e.payload, '$.status') AS status
FROM {{target:oil_dds__event}} e
WHERE e.event_type='well'
    AND NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__event}} i
         WHERE i.event_id=e.event_id);
