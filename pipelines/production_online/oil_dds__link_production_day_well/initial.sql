-- Связываем production business key с well key по исходному payload. Это целевой SQL-этап вместо части apply_batch GP.
SELECT link_hk,
       production_hk,
       well_hk,
       load_dts,
       record_source
FROM {{source:oil_dds__link_production_day_well}};
