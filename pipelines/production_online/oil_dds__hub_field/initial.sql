-- Берём distinct бизнес-ключи месторождений и вычисляем тот же md5 hash key, что и в GP. Это целевой SQL-этап вместо части apply_batch GP.
SELECT field_hk,
       field_id,
       load_dts,
       record_source
FROM {{source:oil_dds__hub_field}};
