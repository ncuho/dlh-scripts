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
FROM {{source:oil_dds__sat_well}};
