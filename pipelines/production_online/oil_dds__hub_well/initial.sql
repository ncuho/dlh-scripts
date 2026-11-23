-- Берём distinct бизнес-ключи скважин; изменяемые названия и статусы остаются в satellites. Это целевой SQL-этап вместо части apply_batch GP.
SELECT well_hk,
       well_id,
       load_dts,
       record_source
FROM {{source:oil_dds__hub_well}};
