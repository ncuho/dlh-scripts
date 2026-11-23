-- Создаём уникальные ключи скважино-дня по согласованному production business ID. Это целевой SQL-этап вместо части apply_batch GP.
SELECT production_hk,
       production_id,
       load_dts,
       record_source
FROM {{source:oil_dds__hub_production_day}};
